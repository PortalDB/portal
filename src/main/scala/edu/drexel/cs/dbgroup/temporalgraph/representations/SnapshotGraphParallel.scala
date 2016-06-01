package edu.drexel.cs.dbgroup.temporalgraph.representations

import org.apache.spark.graphx.lib.ShortestPaths

import scala.collection.parallel.ParSeq
import scala.collection.mutable.Buffer
import scala.reflect.ClassTag
import scala.util.control._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import org.apache.spark.{SparkContext,SparkException,Partition}

import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps

import java.time.LocalDate

class SnapshotGraphParallel[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[((VertexId, VertexId), (Interval, ED))], grphs: ParSeq[Graph[VD,ED]], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraphNoSchema[VD, ED](intvs, verts, edgs, defValue, storLevel, coal) {

  protected val graphs: ParSeq[Graph[VD, ED]] = grphs

  //TODO: we should enforce the integrity constraint
  //by removing edges which connect nonexisting vertices at some time t
  //or throw an exception upon construction

  override def materialize() = {
    allVertices.count
    allEdges.count
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
    //compute indices of start and stop
    val selectStart:Int = intervals.indexWhere(intv => intv.intersects(selectBound))
    var selectStop:Int = intervals.lastIndexWhere(intv => intv.intersects(selectBound))
    if (selectStop < 0) selectStop = intervals.size - 1
    val newIntvs: Seq[Interval] = intervals.slice(selectStart, selectStop+1)

    new SnapshotGraphParallel(newIntvs, allVertices.filter{ case (vid, (intv, attr)) => intv.intersects(selectBound)}.mapValues(y => (Interval(TempGraphOps.maxDate(y._1.start, startBound), TempGraphOps.minDate(y._1.end, endBound)), y._2)), allEdges.filter{ case (vids, (intv, attr)) => intv.intersects(selectBound)}.mapValues(y => (Interval(TempGraphOps.maxDate(y._1.start, startBound), TempGraphOps.minDate(y._1.end, endBound)), y._2)), graphs.slice(selectStart, selectStop+1), defaultValue, storageLevel, coalesced)

  }

  //expects coalesced input
  override protected def aggregateByChange(c: ChangeSpec, vgroupby: (VertexId, VD) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): SnapshotGraphParallel[VD, ED] = {
    val size: Integer = c.num
    var groups: ParSeq[List[(Graph[VD, ED], Interval)]] = if (size > 1) 
      graphs.zip(intervals).foldLeft(List[List[(Graph[VD, ED], Interval)]](), 0) { (r, c) => r match {
        case (head :: tail, num) =>
          if (num < size)  ( (c :: head) :: tail , num + 1 )
          else             ( List(c) :: head :: tail , 1 )
        case (Nil, num) => (List(List(c)), 1)
      }
      }._1.foldLeft(List[List[(Graph[VD, ED], Interval)]]())( (r,c) => c.reverse :: r).par
    else
      graphs.zip(intervals).map(g => List(g))

    implicit val ord = TempGraphOps.dateOrdering
    val combine = (lst: List[Interval]) => lst.sortBy(x => x.start).foldLeft(List[Interval]()){ (r,c) => r match {
      case head :: tail =>
        if (head.intersects(c)) Interval(head.start, TempGraphOps.maxDate(head.end, c.end)) :: tail else c :: head :: tail
      case Nil => List(c)
    }}

    //for each group, reduce into vertices and edges
    //compute new value, filter by quantification
    val reduced: ParSeq[(RDD[(VertexId, (VD, List[Interval]))], RDD[((VertexId, VertexId),(ED, List[Interval]))])] = groups.map(group => 
      group.map{ case (g,ii) => 
        //map each vertex into its new key
        (g.vertices.map{ case (vid, vattr) => (vgroupby(vid, vattr), (vattr, List(ii)))}, 
          g.triplets.map{ e => ((vgroupby(e.srcId, e.srcAttr), vgroupby(e.dstId, e.dstAttr)), (e.attr, List(ii)))}, ii)}
        .reduce((a: (RDD[(VertexId, (VD, List[Interval]))], RDD[((VertexId, VertexId), (ED, List[Interval]))], Interval), b: (RDD[(VertexId, (VD, List[Interval]))], RDD[((VertexId, VertexId),(ED, List[Interval]))], Interval)) => (a._1.union(b._1), a._2.union(b._2), a._3.union(b._3)))
        //reduce by key with aggregate functions
    ).map{ case (vs, es, intv) =>
        (vs.reduceByKey((a,b) => (vAggFunc(a._1, b._1), a._2 ++ b._2))
          .filter(v => vquant.keep(combine(v._2._2).map(ii => ii.ratio(intv)).reduce(_ + _))),
          es.reduceByKey((a,b) => (eAggFunc(a._1, b._1), a._2 ++ b._2))
          .filter(e => equant.keep(combine(e._2._2).map(ii => ii.ratio(intv)).reduce(_ + _)))
        )
    }

    //now we can create new graphs
    //to enforce constraint on edges, subgraph vertices that have default attribute value
    val defval = defaultValue
    val vp = (vid: VertexId, attr: VD) => attr != defval
    val newGraphs: ParSeq[Graph[VD, ED]] = reduced.map { case (vs, es) =>
      Graph(vs.mapValues(v => v._1), es.map(e => Edge(e._1._1, e._1._2, e._2._1)), defaultValue, storageLevel, storageLevel).subgraph(epred = et => true, vpred = vp)
    }

    val newIntervals: Seq[Interval] = intervals.grouped(size).map{group => group.reduce((a,b) => Interval(a.start, b.end))}.toSeq

    SnapshotGraphParallel.fromGraphs(newIntervals, newGraphs, defaultValue, storageLevel)
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
      val newIntvs: Seq[Interval] = TempGraphOps.intervalUnion(intervals, grp2.intervals)

      var ii: Integer = 0
      var jj: Integer = 0
      val empty: Interval = new Interval(LocalDate.MAX, LocalDate.MAX)

      val newGraphs: ParSeq[Graph[VD, ED]] = newIntvs.map { intv =>
        if (intervals.lift(ii).getOrElse(empty).intersects(intv) && grp2.intervals.lift(jj).getOrElse(empty).intersects(intv)) {
          val ret = Graph(graphs(ii).vertices.union(grp2.graphs(jj).vertices).reduceByKey(vFunc), graphs(ii).edges.union(grp2.graphs(jj).edges).map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey(eFunc).map(e => Edge(e._1._1, e._1._2, e._2)), defaultValue, storageLevel, storageLevel)
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
    } else if (span.end == grp2.span.start || span.start == grp2.span.end) {
      //if the two spans are one right after another but do not intersect
      //then we just put them together, but the result may be uncoalesced
      val newIntvs = if (span.start.isBefore(grp2.span.start)) intervals ++ grp2.intervals else grp2.intervals ++ intervals
      val newGraphs = if (span.start.isBefore(grp2.span.start)) graphs ++ grp2.graphs else grp2.graphs ++ graphs
      if (ProgramContext.eagerCoalesce) //fromRDDs will take care of coalescing
        fromRDDs(allVertices.union(grp2.vertices), allEdges.union(grp2.edges), defaultValue, storageLevel, false)
      else
        new SnapshotGraphParallel(newIntvs, allVertices.union(grp2.vertices), allEdges.union(grp2.edges), newGraphs, defaultValue, storageLevel, false)
    } else {
      //if there is no temporal intersection, then we can just add them together
      //no need to worry about coalesce or constraint on E; all still holds
      val newIntvs = if (span.start.isBefore(grp2.span.start)) intervals ++ Seq(Interval(span.end, grp2.span.start)) ++ grp2.intervals else grp2.intervals ++ Seq(Interval(grp2.span.end, span.start)) ++ intervals
      val newGraphs = if (span.start.isBefore(grp2.span.start)) graphs ++ Seq(Graph[VD,ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)) ++ grp2.graphs else grp2.graphs ++ Seq(Graph[VD,ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)) ++ graphs

      new SnapshotGraphParallel(newIntvs, allVertices.union(grp2.vertices), allEdges.union(grp2.edges), newGraphs, defaultValue, storageLevel, coalesced && grp2.coalesced)
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
      val newIntvs: Seq[Interval] = TempGraphOps.intervalIntersect(intervals, grp2.intervals)

      var ii: Integer = 0
      var jj: Integer = 0
      val empty: Interval = new Interval(LocalDate.MAX, LocalDate.MAX)
      while (!intervals.lift(ii).getOrElse(empty).intersects(newIntvs.head)) ii = ii + 1
      while (!grp2.intervals.lift(jj).getOrElse(empty).intersects(newIntvs.head)) jj = jj + 1

      val newGraphs: ParSeq[Graph[VD, ED]] = newIntvs.map { intv =>
        if (intervals.lift(ii).getOrElse(empty).intersects(intv) && grp2.intervals.lift(jj).getOrElse(empty).intersects(intv)) {
          val ret = Graph(graphs(ii).vertices.join(grp2.graphs(jj).vertices).mapValues{ case (a,b) => vFunc(a,b)}, graphs(ii).edges.innerJoin(grp2.graphs(jj).edges)((srcid, dstid, a, b) => eFunc(a,b)), defaultValue, storageLevel, storageLevel)
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
        TGraphNoSchema.coalesce(total.reduce((x: RDD[(VertexId, (Interval, Int))] ,y: RDD[(VertexId, (Interval, Int))]) => x union y))
      else {
        ProgramContext.sc.emptyRDD
      }
    } else {
      ProgramContext.sc.emptyRDD
    }
  }

  //run PageRank on each contained snapshot
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): SnapshotGraphParallel[Double, Double] = {

    val safePagerank = (grp: Graph[VD, ED]) => {
      if (grp.edges.isEmpty) {
        Graph[Double, Double](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        if (!uni) {
          UndirectedPageRank.run(grp, tol, resetProb, numIter)
        } else if (numIter < Int.MaxValue)
          grp.staticPageRank(numIter, resetProb)
        else
          grp.pageRank(tol, resetProb)
      }
    }

    SnapshotGraphParallel.fromGraphs(intervals, graphs.map(safePagerank), 0.0, storageLevel)

  }

  override def connectedComponents(): SnapshotGraphParallel[VertexId, ED] = {
    val safeConnectedComponents = (grp: Graph[VD, ED]) => {
      if (grp.vertices.isEmpty) {
        Graph[VertexId, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        grp.connectedComponents()
      }
    }

    SnapshotGraphParallel.fromGraphs(intervals, graphs.map(safeConnectedComponents), -1, storageLevel)

  }

  override def shortestPaths(uni: Boolean, landmarks: Seq[VertexId]): SnapshotGraphParallel[Map[VertexId, Int], ED] = {
    val safeShortestPaths = (grp: Graph[VD, ED]) => {
      if (grp.vertices.isEmpty) {
        Graph[ShortestPathsXT.SPMap, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        if (!uni)
          ShortestPathsXT.run(grp, landmarks)
        else
          ShortestPaths.run(grp, landmarks)
      }
    }

    SnapshotGraphParallel.fromGraphs(intervals, graphs.map(safeShortestPaths), Map[VertexId,Int](), storageLevel)

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
      }, defaultValue, storageLevel, coalesced)
    } else
      this
  }

  override protected def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false): SnapshotGraphParallel[V, E] = {
    SnapshotGraphParallel.fromRDDs(verts, edgs, defVal, storLevel, coal)
  }

  protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): SnapshotGraphParallel[V, E] = SnapshotGraphParallel.emptyGraph(defVal)
}

object SnapshotGraphParallel extends Serializable {
  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): SnapshotGraphParallel[V, E] = {
    val cverts = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(verts) else verts
    val cedges = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(edgs) else edgs
    val coal = coalesced | ProgramContext.eagerCoalesce

    val intervals = TGraphNoSchema.computeIntervals(cverts, cedges)

    //compute the graphs. due to spark lazy evaluation,
    //if these graphs are not needed, they aren't actually materialized
    val graphs: ParSeq[Graph[V,E]] = intervals.map( p =>
      Graph(cverts.filter(v => v._2._1.intersects(p)).map(v => (v._1, v._2._2)),
        cedges.filter(e => e._2._1.intersects(p)).map(e => Edge(e._1._1, e._1._2, e._2._2)),
        defVal, storLevel, storLevel)).par

    new SnapshotGraphParallel(intervals, cverts, cedges, graphs, defVal, storLevel, coal)

  }

  def fromGraphs[V: ClassTag, E: ClassTag](intervals: Seq[Interval], graphs: ParSeq[Graph[V, E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY): SnapshotGraphParallel[V, E] = {
    val verts: RDD[(VertexId, (Interval, V))] = graphs.zip(intervals).map{ case (g,i) => g.vertices.map{ case (vid, attr) => (vid, (i, attr))}}.reduce((a, b) => a union b)
    val edges: RDD[((VertexId, VertexId), (Interval, E))] = graphs.zip(intervals).map{ case (g,i) => g.edges.map(e => ((e.srcId, e.dstId), (i, e.attr)))}.reduce((a, b) => a union b)

    if (ProgramContext.eagerCoalesce)
      fromRDDs(verts, edges, defVal, storLevel, false)
    else
      new SnapshotGraphParallel(intervals, verts, edges, graphs, defVal, storLevel, false)
  }

  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V):SnapshotGraphParallel[V, E] = new SnapshotGraphParallel(Seq[Interval](), ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, ParSeq[Graph[V,E]](), defVal, coal = true)
 
}
