package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.parallel.ParSeq
import scala.collection.mutable.Buffer
import scala.reflect.ClassTag
import scala.util.control._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import org.apache.spark.{SparkContext,SparkException,Partition}

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

  def aggregate(res: WindowSpecification, vgroupby: (VertexId, VD) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraph[VD, ED] = {
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
      case c : ChangeSpec => aggregateByChange(c, vgroupby, vquant, equant, vAggFunc, eAggFunc)
      case t : TimeSpec => aggregateByTime(t, vgroupby, vquant, equant, vAggFunc, eAggFunc)
      case _ => throw new IllegalArgumentException("unsupported window specification")
    }
  }

  protected def aggregateByChange(c: ChangeSpec, vgroupby: (VertexId, VD) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraph[VD, ED] = {
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
          es.map{ e => ((vgroupby(e.srcId, e.srcAttr), vgroupby(e.dstId, e.dstAttr)), (e.attr, 1))}
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

    SnapshotGraphParallel.fromGraphs(newIntervals, newGraphs, defaultValue, storLevel)
  }

  protected def aggregateByTime(c: TimeSpec, vgroupby: (VertexId, VD) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraph[VD, ED] = {
    val start = span.start
    //TODO: if there is no structural aggregation, i.e. vgroupby is vid => vid
    //then we can skip the expensive joins

    //for each vertex, we split it into however many intervals it falls into
    //using the new id from vgroupby
    val splitVerts: RDD[((VertexId, Interval), (VD, Double))] = allVertices.flatMap{ case (vid, (intv, attr)) => intv.split(c.res, start).map(ii => ((vgroupby(vid,attr), ii._3), (attr, ii._2)))}
    val newVIds: RDD[(VertexId, (Interval, VertexId))] = allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, vgroupby(vid, attr)))}

    //for each edge, similar except computing the new ids requires joins with V
    val edgesWithIds: RDD[((VertexId, VertexId), (Interval, ED))] = allEdges.map(e => (e._1._1, e)).join(newVIds).filter{ case (vid, (e, v)) => e._2._1.intersects(v._1)}.map{ case (vid, (e, v)) => (e._1._2, (vid, (Interval(maxDate(e._2._1.start, v._1.start), minDate(e._2._1.end, v._1.end)), e._2._2)))}.join(newVIds).filter{ case (vid, (e, v)) => e._2._1.intersects(v._1)}.map{ case (vid, (e, v)) => ((e._1, vid), (Interval(maxDate(e._2._1.start, v._1.start), minDate(e._2._1.end, v._1.end)), e._2._2))}
    val splitEdges: RDD[((VertexId, VertexId, Interval),(ED, Double))] = edgesWithIds.flatMap{ case (ids, (intv, attr)) => intv.split(c.res, start).map(ii => ((ids._1, ids._2, ii._3), (attr, ii._2)))}

    //reduce vertices by key, also computing the total period occupied
    //filter out those that do not meet quantification criteria
    //map to final result
    val newVerts: RDD[(VertexId, (Interval, VD))] = splitVerts.reduceByKey((a,b) => (vAggFunc(a._1, b._1), a._2 + b._2)).filter(v => vquant.keep(v._2._2)).map(v => (v._1._1, (v._1._2, v._2._1)))
    //same for edges
    val aggEdges: RDD[((VertexId, VertexId), (Interval, ED))] = splitEdges.reduceByKey((a,b) => (eAggFunc(a._1, b._1), a._2 + b._2)).filter(e => equant.keep(e._2._2)).map(e => ((e._1._1, e._1._2), (e._1._3, e._2._1)))
    val newEdges = if (aggEdges.isEmpty) aggEdges else constrainEdges(newVerts, aggEdges)

    SnapshotGraphParallel.fromRDDs(newVerts, newEdges, defaultValue, storageLevel)
  }

  override def project[ED2: ClassTag, VD2: ClassTag](emap: (Edge[ED], Interval) => ED2, vmap: (VertexId, Interval, VD) => VD2, defVal: VD2): SnapshotGraphParallel[VD2, ED2] = {
    //project may cause coalesce but it does not affect the integrity constraint on E
    //so we don't need to check it
    SnapshotGraphParallel.fromRDDs(TGraphNoSchema.coalesce(allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, vmap(vid, intv, attr)))}), TGraphNoSchema.coalesce(allEdges.map{ case (ids, (intv, attr)) => (ids, (intv, emap(Edge(ids._1, ids._2, attr), intv)))}), defVal, storageLevel)
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2, defVal: VD2)(implicit eq: VD =:= VD2 = null): SnapshotGraphParallel[VD2, ED] = {
    SnapshotGraphParallel.fromRDDs(TGraphNoSchema.coalesce(allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, map(vid, intv, attr)))}), allEdges, defaultValue, storageLevel)
  }

  override def mapEdges[ED2: ClassTag](map: (Edge[ED], Interval) => ED2): SnapshotGraphParallel[VD, ED2] = {
    SnapshotGraphParallel.fromRDDs(allVertices, TGraphNoSchema.coalesce(allEdges.map{ case (ids, (intv, attr)) => (ids, (intv, map(Edge(ids._1, ids._2, attr), intv)))}), defaultValue, storageLevel)
  }

  override def union(other: TGraph[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): SnapshotGraphParallel[VD, ED] = {
    var grp2: SnapshotGraphParallel[VD, ED] = other match {
      case grph: SnapshotGraphParallel[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (span.intersects(grp2.span)) {
      //there are two methods to do this:
      //A: by graphs. union by pairs of each two graphs that have the same periods
      //then reducebykey to compute attribute value
      //B: by tuples using fullouterjoin

      //this is method A
      //compute new intervals
      val newIntvs: Seq[Interval] = intervalUnion(grp2.intervals)

      var ii: Integer = 0
      var jj: Integer = 0
      val empty: Interval = new Interval(LocalDate.MAX, LocalDate.MAX)

      val newGraphs: ParSeq[Graph[VD, ED]] = newIntvs.map { intv =>
        if (intervals.lift(ii).getOrElse(empty).intersects(intv) && grp2.intervals.lift(jj).getOrElse(empty).intersects(intv)) {
          val ret = Graph(graphs(ii).vertices.union(grp2.graphs(jj).vertices).reduceByKey(vFunc), graphs(ii).edges.union(grp2.graphs(jj).edges).map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey(eFunc).map(e => Edge(e._1._1, e._1._2, e._2)), defaultValue, storageLevel)
          if (intervals(ii).end == intv.end)
            ii = ii+1
          if (grp2.intervals(jj).end == intv.end)
            jj = jj+1
          ret
        } else if (intervals.lift(ii).getOrElse(empty).intersects(intv)) {
          if (intervals(ii).end == intv.end)
            ii = ii+1
          graphs(ii-1)
        } else if (grp2.intervals.lift(jj).getOrElse(empty).intersects(intv)) {
          if (grp2.intervals(jj).end == intv.end)
            jj = jj+1
          grp2.graphs(jj-1)
        } else { //should never get here
          throw new SparkException("bug in union")
        }
      }.par

      SnapshotGraphParallel.fromGraphs(newIntvs, newGraphs, defaultValue, storageLevel)

    } else {
      //if there is no temporal intersection, then we can just add them together
      //no need to worry about coalesce or constraint on E; all still holds
      SnapshotGraphParallel.fromRDDs(allVertices.union(grp2.vertices), allEdges.union(grp2.edges), defaultValue, storageLevel)
    }
  }

  override def intersection(other: TGraph[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): SnapshotGraphParallel[VD, ED] = {
    var grp2: SnapshotGraphParallel[VD, ED] = other match {
      case grph: SnapshotGraphParallel[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (span.intersects(grp2.span)) {
      //similar to union, there are two ways to do this
      //by graphs or by tuples with join
      //this is by graphs

      //compute new intervals
      val newIntvs: Seq[Interval] = intervalIntersect(grp2.intervals)

      var ii: Integer = 0
      var jj: Integer = 0
      val empty: Interval = new Interval(LocalDate.MAX, LocalDate.MAX)
      while (!intervals.lift(ii).getOrElse(empty).intersects(newIntvs.head)) ii = ii + 1
      while (!grp2.intervals.lift(jj).getOrElse(empty).intersects(newIntvs.head)) jj = jj + 1

      val newGraphs: ParSeq[Graph[VD, ED]] = newIntvs.map { intv =>
        if (intervals.lift(ii).getOrElse(empty).intersects(intv) && grp2.intervals.lift(jj).getOrElse(empty).intersects(intv)) {
          val ret = Graph(graphs(ii).vertices.join(grp2.graphs(jj).vertices).mapValues{ case (a,b) => vFunc(a,b)}, graphs(ii).edges.innerJoin(grp2.graphs(jj).edges)((srcid, dstid, a, b) => eFunc(a,b)), defaultValue, storageLevel)
          if (intervals(ii).end == intv.end)
            ii = ii+1
          if (grp2.intervals(jj).end == intv.end)
            jj = jj+1
          ret
        } else { //should never get here
          throw new SparkException("bug in intersection")
        }
      }.par

      SnapshotGraphParallel.fromGraphs(newIntvs, newGraphs, defaultValue, storageLevel)

    } else {
      SnapshotGraphParallel.emptyGraph(defaultValue)
    }
  }

  override def pregel[A: ClassTag]
     (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
       sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): SnapshotGraphParallel[VD, ED] = {
    SnapshotGraphParallel.fromGraphs(intervals, graphs.map(x => Pregel(x, initialMsg,
      maxIterations, activeDirection)(vprog, sendMsg, mergeMsg)), defaultValue, storageLevel)
  }

  override def degree: RDD[(VertexId, (Interval, Int))] = {
    if (!allEdges.isEmpty) {
      val total = graphs.zip(intervals)
        .filterNot(x => x._1.edges.isEmpty)
        .map(x => x._1.degrees.mapValues(deg => (x._2,deg)))
      if (total.size > 0)
        TGraphNoSchema.coalesce(total.reduce((x,y) => VertexRDD(x union y)))
      else {
        ProgramContext.sc.emptyRDD
      }
    } else {
      ProgramContext.sc.emptyRDD
    }
  }

  //run PageRank on each contained snapshot
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): RDD[(VertexId, (Interval, Double))] = {

    def safePagerank(grp: Graph[VD, ED]): Graph[Double, Double] = {
      if (grp.edges.isEmpty) {
        Graph[Double, Double](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        if (uni) {
          UndirectedPageRank.run(grp, tol, resetProb, numIter)
        } else if (numIter < Int.MaxValue)
          grp.staticPageRank(numIter, resetProb)
        else
          grp.pageRank(tol, resetProb)
      }
    }

    TGraphNoSchema.coalesce(graphs.map(safePagerank).zip(intervals).map{case (g,intv) => g.vertices.mapValues((vid, attr) => (intv, attr))}.reduce((a: RDD[(VertexId, (Interval, Double))], b: RDD[(VertexId, (Interval, Double))]) => a.union(b)))

  }

  override def connectedComponents(): RDD[(VertexId, (Interval, VertexId))] = {
    def safeConnectedComponents(grp: Graph[VD, ED]): Graph[VertexId, ED] = {
      if (grp.vertices.isEmpty) {
        Graph[VertexId, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        grp.connectedComponents()
      }
    }

    TGraphNoSchema.coalesce(graphs.map(safeConnectedComponents).zip(intervals).map{case (g,intv) => g.vertices.mapValues((vid, attr) => (intv, attr))}.reduce((a: RDD[(VertexId, (Interval, VertexId))], b: RDD[(VertexId, (Interval, VertexId))]) => a.union(b)))

  }

  override def shortestPaths(landmarks: Seq[VertexId]): RDD[(VertexId, (Interval, Map[VertexId, Int]))] = {
    def safeShortestPaths(grp: Graph[VD, ED]): Graph[ShortestPathsXT.SPMap, ED] = {
      if (grp.vertices.isEmpty) {
        Graph[ShortestPathsXT.SPMap, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        ShortestPathsXT.run(grp, landmarks)
      }
    }

    TGraphNoSchema.coalesce(graphs.map(safeShortestPaths).zip(intervals).map{case (g,intv) => g.vertices.mapValues((vid, attr) => (intv, attr))}.reduce((a: RDD[(VertexId, (Interval, Map[VertexId, Int]))], b: RDD[(VertexId, (Interval, Map[VertexId, Int]))]) => a.union(b)))

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
    if (pst != PartitionStrategyType.None) {
      //not changing the intervals, only the graphs at their indices
      //each partition strategy for SG needs information about the graph

      //use that strategy to partition each of the snapshots
      new SnapshotGraphParallel(intervals, allVertices, allEdges, graphs.zipWithIndex.map { case (g,i) =>
        val numParts: Int = if (parts > 0) parts else g.edges.partitions.size
        g.partitionBy(PartitionStrategies.makeStrategy(pst, i, graphs.size, runs), numParts)
      }, defaultValue, storageLevel)
    } else
      this
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
