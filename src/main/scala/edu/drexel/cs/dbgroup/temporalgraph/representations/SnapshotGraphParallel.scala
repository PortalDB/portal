package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.parallel.ParSeq
import scala.collection.mutable.Buffer
import scala.reflect.ClassTag
import scala.util.control._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import org.apache.spark.SparkContext
import org.apache.spark.Partition

import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphLoaderAddon
import org.apache.spark.rdd._
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.{MultifileLoad,TempGraphOps,NumberRangeRegex}
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps._

import java.time.LocalDate

class SnapshotGraphParallel[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[((VertexId, VertexId), (Interval, ED))], grphs: ParSeq[Graph[VD,ED]], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY) extends TGraphNoSchema[VD, ED](intvs, verts, edgs) with Serializable {

  val storageLevel = storLevel
  val defaultValue: VD = defValue
  private val graphs: ParSeq[Graph[VD, ED]] = grphs

  //TODO: we should enforce the integrity constraint
  //by removing edges which connect nonexisting vertices at some time t
  //or throw an exception upon construction

  override def materialize() = {
    graphs.foreach { x =>
      if (!x.edges.isEmpty)
        x.numEdges
      if (!x.vertices.isEmpty)
        x.numVertices
    }
  }

  override def getSnapshot(time: LocalDate): Graph[VD,ED] = {
    val index = intervals.indexWhere(a => a.contains(time))
    if (index >= 0) {
      graphs(index)
    } else
      Graph[VD,ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
  }

  /** Query operations */

  override def slice(bound: Interval): SnapshotGraphParallel[VD, ED] = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this

    if (!span.intersects(bound)) {
      return SnapshotGraphParallel.emptyGraph[VD,ED](defaultValue)
    }

    val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
    val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end
    val selectBound:Interval = Interval(startBound, endBound)

    SnapshotGraphParallel.fromRDDs(allVertices.filter{ case (vid, (intv, attr)) => intv.intersects(selectBound)}.mapValues(y => (Interval(maxDate(y._1.start, startBound), minDate(y._1.end, endBound)), y._2)), allEdges.filter{ case (vids, (intv, attr)) => intv.intersects(selectBound)}.mapValues(y => (Interval(maxDate(y._1.start, startBound), minDate(y._1.end, endBound)), y._2)), defaultValue, storageLevel)
  }

  override def select(vtpred: Interval => Boolean, etpred: Interval => Boolean): SnapshotGraphParallel[VD, ED] = {
    //because of the integrity constraint on edges, they have to 
    //satisfy both predicates
    SnapshotGraphParallel.fromRDDs(allVertices.filter{ case (vid, (intv, attr)) => vtpred(intv)}, allEdges.filter{ case (ids, (intv, attr)) => vtpred(intv) && etpred(intv)}, defaultValue, storageLevel)

  }

  override def select(epred: ((VertexId, VertexId), (Interval, ED)) => Boolean = (ids, attrs) => true, vpred: (VertexId, (Interval, VD)) => Boolean = (vid, attrs) => true): SnapshotGraphParallel[VD, ED] = {
    //TODO: if the vpred is not provided, i.e. is true
    //then we can skip most of the work on enforcing integrity constraints with V

    //can do this in two ways:
    //1. subgraph each representative graph, which takes care of integrity constraint on E, then pull new vertex and edge rdds, and coalesce
    //2. simple select on vertices, then join the coalesced by structure result
    //to modify edges

    //this is method 2
    val newVerts: RDD[(VertexId, (Interval, VD))] = allVertices.filter{ case (vid, attrs) => vpred(vid, attrs)}
    val filteredEdges: RDD[((VertexId, VertexId), (Interval, ED))] = allEdges.filter{ case (ids, attrs) => epred(ids, attrs)}

    val newEdges = if (filteredEdges.isEmpty) filteredEdges else constrainEdges(newVerts, filteredEdges)

    //no need to coalesce either vertices or edges because we are removing some entities, but not extending them or modifying attributes

    SnapshotGraphParallel.fromRDDs(newVerts, newEdges, defaultValue, storageLevel)

  }

  def aggregate(res: WindowSpecification, vgroupby: (VertexId, VD) => VertexId, egroupby: EdgeTriplet[VD, ED] => (VertexId, VertexId), vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraph[VD, ED] = {
    if (allVertices.isEmpty)
      return SnapshotGraphParallel.emptyGraph[VD,ED](defaultValue)

    /* for aggregation by change, there are two ways to do this:
     * 1. aggregate consecutive graphs by the required number of changes. within each group, map entities with new keys, and finally reducebykey
     * 2. compute intervals, then use the same method as for aggregate by time
     * 
     * for aggregation by time, split each entity into as many as there are periods into which it falls, with new id based on groupby,
     * reduce by key, and finally filter out those that don't meet quantification criteria. coalesce.
     */
    res match {
      case c : ChangeSpec => aggregateByChange(c, vgroupby, egroupby, vquant, equant, vAggFunc, eAggFunc)
      case t : TimeSpec => aggregateByTime(t, vgroupby, egroupby, vquant, equant, vAggFunc, eAggFunc)
      case _ => throw new IllegalArgumentException("unsupported window specification")
    }
  }

  protected def aggregateByChange(c: ChangeSpec, vgroupby: (VertexId, VD) => VertexId, egroupby: EdgeTriplet[VD, ED] => (VertexId, VertexId), vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraph[VD, ED] = {
    val size: Integer = c.num
    var groups: ParSeq[List[Graph[VD, ED]]] = if (size > 1) 
      graphs.foldLeft(List[List[Graph[VD, ED]]](), 0) { (r, c) => r match {
        case (head :: tail, num) =>
          if (num < size)  ( (c :: head) :: tail , num + 1 )
          else             ( List(c) :: head :: tail , 1 )
        case (Nil, num) => (List(List(c)), 1)
      }
      }._1.foldLeft(List[List[Graph[VD, ED]]]())( (r,c) => c.reverse :: r).par
    else
      graphs.map(g => List(g))

    //drop the last group if we have Always quantification
    groups = vquant match {
      case a: Always if (groups.last.length < size) => groups.take(groups.length - 1)
      case _ => groups

    }

    //for each group, reduce into vertices and edges
    //compute new value, filter by quantification
    val reduced: ParSeq[(RDD[(VertexId, (VD, Int))], RDD[((VertexId, VertexId),(ED, Int))])] = groups.map(group => group.map(g => (g.vertices, g.triplets))
      .reduce((a: (RDD[(VertexId, VD)], RDD[EdgeTriplet[VD,ED]]), b: (RDD[(VertexId, VD)], RDD[EdgeTriplet[VD,ED]])) => (a._1.union(b._1), a._2.union(b._2)))
      //map each vertex into its new key
      //reduce by key with aggregate functions
    ).map{ case (vs, es) => 
        (vs.map{ case (vid, vattr) => (vgroupby(vid, vattr), (vattr, 1))}
          .reduceByKey((a,b) => (vAggFunc(a._1, b._1), a._2 + b._2))
          .filter(v => vquant.keep(v._2._2 / size.toDouble)),
          es.map{ e => (egroupby(e), (e.attr, 1))}
            .reduceByKey((a,b) => (eAggFunc(a._1, b._1), a._2 + b._2))
          .filter(e => equant.keep(e._2._2 / size.toDouble))
        )
    }

    //now we can create new graphs
    //to enforce constraint on edges, subgraph vertices that have default attribute value
    val newGraphs: ParSeq[Graph[VD, ED]] = reduced.map { case (vs, es) =>
      Graph(vs.mapValues(v => v._1), es.map(e => Edge(e._1._1, e._1._2, e._2._1)), defaultValue, storLevel, storLevel).subgraph(vpred = (vid, attr) => attr != defValue)
    }
    val newIntervals: Seq[Interval] = intervals.grouped(size).map{group => group.reduce((a,b) => Interval(a.start, b.end))}.toSeq

    return SnapshotGraphParallel.fromGraphs(newIntervals, newGraphs, defaultValue, storLevel)
  }

  protected def aggregateByTime(c: TimeSpec, vgroupby: (VertexId, VD) => VertexId, egroupby: EdgeTriplet[VD, ED] => (VertexId, VertexId), vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraph[VD, ED] = {

    return SnapshotGraphParallel.emptyGraph[VD,ED](defaultValue)
  }

  override def project[ED2: ClassTag, VD2: ClassTag](emap: (Edge[ED], Interval) => ED2, vmap: (VertexId, Interval, VD) => VD2): SnapshotGraphParallel[VD2, ED2] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2)(implicit eq: VD =:= VD2 = null): SnapshotGraphParallel[VD2, ED] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def mapEdges[ED2: ClassTag](map: (Edge[ED], Interval) => ED2): SnapshotGraphParallel[VD, ED2] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, Map[Interval, U])])(mapFunc: (VertexId, Interval, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): SnapshotGraphParallel[VD2, ED] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def union(other: TGraph[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): SnapshotGraphParallel[VD, ED] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def intersection(other: TGraph[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): SnapshotGraphParallel[VD, ED] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def pregel[A: ClassTag]
     (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
       sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): SnapshotGraphParallel[VD, ED] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def degree: RDD[(VertexId, Map[Interval, Int])] = {
    if (!allEdges.isEmpty) {
      val total = graphs.zipWithIndex
        .map(x => (x._1, intervals(x._2)))
        .filterNot(x => x._1.edges.isEmpty)
        .map(x => x._1.degrees.mapValues(deg => Map[Interval, Int](x._2 -> deg)))
      if (total.size > 0)
        total.reduce((x,y) => VertexRDD(x union y))
          .reduceByKey((a: Map[Interval, Int], b: Map[Interval, Int]) => a ++ b)
      else {
        ProgramContext.sc.emptyRDD
      }
    } else {
      ProgramContext.sc.emptyRDD
    }
  }

  //run PageRank on each contained snapshot
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): RDD[(VertexId, Map[Interval, Double])] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def connectedComponents(): RDD[(VertexId, Map[Interval, VertexId])] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def shortestPaths(landmarks: Seq[VertexId]): RDD[(VertexId, Map[Interval, Map[VertexId, Int]])] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    graphs.filterNot(_.edges.isEmpty).map(_.edges.partitions.size).reduce(_ + _)
  }

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): SnapshotGraphParallel[VD, ED] = {
    super.persist(newLevel)
    //persist each graph
    //this will throw an exception if the graphs are already persisted
    //with a different storage level
    graphs.map(g => g.persist(newLevel))
    this
  }

  override def unpersist(blocking: Boolean = true): SnapshotGraphParallel[VD, ED] = {
    super.unpersist(blocking)
    graphs.map(_.unpersist(blocking))
    this
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int): SnapshotGraphParallel[VD, ED] = {
    partitionBy(pst, runs, 0)
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): SnapshotGraphParallel[VD, ED] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

}

object SnapshotGraphParallel extends Serializable {
  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY): SnapshotGraphParallel[V, E] = {
    val intervals = TGraphNoSchema.computeIntervals(verts, edgs)

    //compute the graphs. due to spark lazy evaluation,
    //if these graphs are not needed, they aren't actually materialized
    val graphs: ParSeq[Graph[V,E]] = intervals.map( p =>
      Graph(verts.filter(v => v._2._1.intersects(p)).map(v => (v._1, v._2._2)),
        edgs.filter(e => e._2._1.intersects(p)).map(e => Edge(e._1._1, e._1._2, e._2._2)),
        defVal, storLevel, storLevel)).par

    new SnapshotGraphParallel(intervals, verts, edgs, graphs, defVal, storLevel)

  }

  def fromGraphs[V: ClassTag, E: ClassTag](intervals: Seq[Interval], graphs: ParSeq[Graph[V, E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY): SnapshotGraphParallel[V, E] = {

    val verts: RDD[(VertexId, (Interval, V))] = TGraphNoSchema.coalesce(graphs.zip(intervals).map{ case (g,i) => g.vertices.map{ case (vid, attr) => (vid, (i, attr))}}.reduce((a, b) => a union b))
    val edges: RDD[((VertexId, VertexId), (Interval, E))] = TGraphNoSchema.coalesce(graphs.zip(intervals).map{ case (g,i) => g.edges.map(e => ((e.srcId, e.dstId), (i, e.attr)))}.reduce((a, b) => a union b))

    new SnapshotGraphParallel(intervals, verts, edges, graphs, defVal, storLevel)
  }

  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V):SnapshotGraphParallel[V, E] = new SnapshotGraphParallel(Seq[Interval](), ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, ParSeq[Graph[V,E]](), defVal)
 
}
