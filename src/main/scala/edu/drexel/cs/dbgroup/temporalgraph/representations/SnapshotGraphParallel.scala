package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.JavaConversions._
import scala.collection.parallel.ParSeq
import scala.collection.mutable.Buffer
import scala.reflect.ClassTag
import scala.util.control._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.{Partition, SparkContext, SparkException}
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.graphx.lib.ShortestPaths

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps

import java.time.LocalDate
import java.util.Map
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap

class SnapshotGraphParallel[VD: ClassTag, ED: ClassTag](intvs: RDD[Interval], verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[((VertexId, VertexId), (Interval, ED))], grphs: ParSeq[Graph[VD,ED]], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraphNoSchema[VD, ED](intvs, verts, edgs, defValue, storLevel, coal) {

  protected var graphs: ParSeq[Graph[VD, ED]] = grphs
  protected var partitioning = TGraphPartitioning(PartitionStrategyType.None, 1, 0)

  //TODO: we should enforce the integrity constraint
  //by removing edges which connect nonexisting vertices at some time t
  //or throw an exception upon construction

  override def materialize() = {
    if (graphs.size < 1) computeGraphs()
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
    if (graphs.size > 0) {
      val index = intervals.zipWithIndex.filter(g => g._1.contains(time))
      if (!index.isEmpty) {
        graphs(index.first._2.toInt)
      } else
        Graph[VD,ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
    } else super.getSnapshot(time)
  }

  /** Query operations */

  override def slice(bound: Interval): SnapshotGraphParallel[VD, ED] = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this

    if (!span.intersects(bound)) {
      return SnapshotGraphParallel.emptyGraph[VD,ED](defaultValue)
    }

    if (graphs.size < 1) computeGraphs()
    val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
    val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end
    val selectBound:Interval = Interval(startBound, endBound)
    //compute indices of start and stop
    val zipped = intervals.zipWithIndex.filter(intv => intv._1.intersects(selectBound))
    val selectStart:Int = zipped.min._2.toInt
    val selectStop:Int = zipped.max._2.toInt
    val newIntvs: RDD[Interval] = zipped.map(x => x._1)

    new SnapshotGraphParallel(newIntvs, allVertices.filter{ case (vid, (intv, attr)) => intv.intersects(selectBound)}.mapValues(y => (Interval(TempGraphOps.maxDate(y._1.start, startBound), TempGraphOps.minDate(y._1.end, endBound)), y._2)), allEdges.filter{ case (vids, (intv, attr)) => intv.intersects(selectBound)}.mapValues(y => (Interval(TempGraphOps.maxDate(y._1.start, startBound), TempGraphOps.minDate(y._1.end, endBound)), y._2)), graphs.slice(selectStart, selectStop+1), defaultValue, storageLevel, coalesced)

  }

  //expects coalesced input
  override protected def aggregateByChange(c: ChangeSpec, vgroupby: (VertexId, VD) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): SnapshotGraphParallel[VD, ED] = {
    val size: Integer = c.num
    if (graphs.size < 1) computeGraphs()
    //TODO: rewrite to use the RDD insteand of seq
    val intervalsc = intervals.collect
    var groups: ParSeq[List[(Graph[VD, ED], Interval)]] = if (size > 1) 
      graphs.zip(intervalsc).foldLeft(List[List[(Graph[VD, ED], Interval)]](), 0) { (r, c) => r match {
        case (head :: tail, num) =>
          if (num < size)  ( (c :: head) :: tail , num + 1 )
          else             ( List(c) :: head :: tail , 1 )
        case (Nil, num) => (List(List(c)), 1)
      }
      }._1.foldLeft(List[List[(Graph[VD, ED], Interval)]]())( (r,c) => c.reverse :: r).par
    else
      graphs.zip(intervalsc).map(g => List(g))

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
    val vp = (vid: VertexId, attr: VD) => attr != null
    val newGraphs: ParSeq[Graph[VD, ED]] = reduced.map { case (vs, es) =>
      val g = Graph(vs.mapValues(v => v._1), es.map(e => Edge(e._1._1, e._1._2, e._2._1)), null.asInstanceOf[VD], storageLevel, storageLevel)
      if (vquant.threshold <= equant.threshold) g else g.subgraph(epred = et => true, vpred = vp)
    }

    val newIntervals: RDD[Interval] = intervals.zipWithIndex.map(x => ((x._2 / size), x._1)).reduceByKey((a,b) => Interval(TempGraphOps.minDate(a.start, b.start), TempGraphOps.maxDate(a.end, b.end))).sortBy(c => c._1, true).map(x => x._2)

    SnapshotGraphParallel.fromGraphs(newIntervals, newGraphs, defaultValue, storageLevel)
  }

  override def project[ED2: ClassTag, VD2: ClassTag](emap: Edge[ED] => ED2, vmap: (VertexId, VD) => VD2, defVal: VD2): SnapshotGraphParallel[VD2, ED2] = {
    if (graphs.size < 1) computeGraphs()
    SnapshotGraphParallel.fromGraphs(intervals, graphs.map(g => g.mapVertices(vmap).mapEdges(emap)), defVal, storageLevel)
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2, defVal: VD2)(implicit eq: VD =:= VD2 = null): SnapshotGraphParallel[VD2, ED] = {
    if (graphs.size < 1) computeGraphs()
    SnapshotGraphParallel.fromGraphs(intervals, graphs.zip(intervals.collect).map(g => g._1.mapVertices((vid, attr) => map(vid, g._2, attr))), defVal, storageLevel)
  }

  override def mapEdges[ED2: ClassTag](map: (Interval, Edge[ED]) => ED2): SnapshotGraphParallel[VD, ED2] = {
    if (graphs.size < 1) computeGraphs()
    SnapshotGraphParallel.fromGraphs(intervals, graphs.zip(intervals.collect).map(g => g._1.mapEdges(e => map(g._2, e))), defaultValue, storageLevel)
  }

  override def union(other: TGraph[VD, ED]): SnapshotGraphParallel[Set[VD], Set[ED]] = {
    var grp2: SnapshotGraphParallel[VD, ED] = other match {
      case grph: SnapshotGraphParallel[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (graphs.size < 1) computeGraphs()
    if (grp2.graphs.size < 1) grp2.computeGraphs()

    if (span.intersects(grp2.span)) {
      //there are two methods to do this:
      //A: by graphs. union by pairs of each two graphs that have the same periods
      //then reducebykey to compute attribute value
      //B: by tuples using fullouterjoin

      //this is method A
      //compute new intervals
      val newIntvs: RDD[Interval] = TempGraphOps.intervalUnion(intervals, grp2.intervals)

      var ii: Integer = 0
      var jj: Integer = 0
      val empty: Interval = new Interval(LocalDate.MAX, LocalDate.MAX)
      implicit val ord: Ordering[Interval] = Ordering.fromLessThan((a,b) => a.start.isBefore(b.start))

      //TODO: get rid of collect if possible
      val intervalsZipped = intervals.zipWithIndex.map(_.swap)
      val intervals2Zipped = grp2.intervals.zipWithIndex.map(_.swap)
      val newGraphs: ParSeq[Graph[Set[VD],Set[ED]]] = newIntvs.collect.map { intv =>
        val iith = intervalsZipped.lookup(ii.toLong).lift(0).getOrElse(empty)
        val jjth = intervals2Zipped.lookup(jj.toLong).lift(0).getOrElse(empty)
        if (iith.intersects(intv) && jjth.intersects(intv)) {
          val ret: Graph[Set[VD], Set[ED]] = Graph(graphs(ii).vertices.fullOuterJoin(grp2.graphs(jj).vertices).mapValues{ attr => (attr._1.toList ++ attr._2.toList).toSet}, 
            graphs(ii).edges.map(e => ((e.srcId, e.dstId), e.attr)).fullOuterJoin(grp2.graphs(jj).edges.map(e => ((e.srcId, e.dstId), e.attr))).map(e => Edge(e._1._1, e._1._2, (e._2._1.toList ++ e._2._2.toList).toSet)),
            Set(defaultValue), storageLevel, storageLevel)
          if (iith.end == intv.end)
            ii = ii+1
          if (jjth.end == intv.end)
            jj = jj+1
          ret
        } else if (iith.intersects(intv)) {
          if (iith.end == intv.end)
            ii = ii+1
          graphs(ii-1).mapVertices((vid,attr) => Set(attr)).mapEdges(e => Set(e.attr))
        } else if (jjth.intersects(intv)) {
          if (jjth.end == intv.end)
            jj = jj+1
          grp2.graphs(jj-1).mapVertices((vid,attr) => Set(attr)).mapEdges(e => Set(e.attr))
        } else { //should never get here
          throw new SparkException("bug in union")
        }
      }.par

      SnapshotGraphParallel.fromGraphs(newIntvs, newGraphs, Set(defaultValue), storageLevel)
    } else if (span.end == grp2.span.start || span.start == grp2.span.end) {
      //if the two spans are one right after another but do not intersect
      //then we just put them together
      val newIntvs = intervals.union(grp2.intervals).sortBy(c => c, true, 1)
      //need to update values for all vertices and edges
      val gr1: ParSeq[Graph[Set[VD],Set[ED]]] = graphs.map(g => g.mapVertices((vid,attr) => Set(attr)).mapEdges(e => Set(e.attr)))
      val gr2: ParSeq[Graph[Set[VD],Set[ED]]] = grp2.graphs.map(g => g.mapVertices((vid,attr) => Set(attr)).mapEdges(e => Set(e.attr)))
      val newGraphs = if (span.start.isBefore(grp2.span.start)) gr1 ++ gr2 else gr2 ++ gr1
      val verts1: RDD[(VertexId, (Interval, Set[VD]))] = allVertices.mapValues{ case (intv, attr) => (intv, Set(attr))}
      val verts2: RDD[(VertexId, (Interval, Set[VD]))] = grp2.allVertices.mapValues{ case (intv, attr) => (intv, Set(attr))}
      val edg1: RDD[((VertexId,VertexId),(Interval,Set[ED]))] = allEdges.mapValues{ case (intv, attr) => (intv, Set(attr))}
      val edg2: RDD[((VertexId,VertexId),(Interval,Set[ED]))] = grp2.allEdges.mapValues{ case (intv, attr) => (intv, Set(attr))}
      new SnapshotGraphParallel(newIntvs, verts1.union(verts2), edg1.union(edg2), newGraphs, Set(defaultValue), storageLevel, false)
    } else {
      //if there is no temporal intersection, then we can just add them together
      //no need to worry about coalesce or constraint on E; all still holds
      val newIntvs = intervals.union(grp2.intervals).union(ProgramContext.sc.parallelize(Seq(Interval(span.end, grp2.span.start)))).sortBy(c => c, true)
      //need to update values for all vertices and edges
      val gr1: ParSeq[Graph[Set[VD],Set[ED]]] = graphs.map(g => g.mapVertices((vid,attr) => Set(attr)).mapEdges(e => Set(e.attr)))
      val gr2: ParSeq[Graph[Set[VD],Set[ED]]] = grp2.graphs.map(g => g.mapVertices((vid,attr) => Set(attr)).mapEdges(e => Set(e.attr)))
      val newGraphs = if (span.start.isBefore(grp2.span.start)) gr1 ++ Seq(Graph[Set[VD],Set[ED]](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)) ++ gr2 else gr2 ++ Seq(Graph[Set[VD],Set[ED]](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)) ++ gr1
      //the union of coalesced is coalesced
      val verts1: RDD[(VertexId, (Interval, Set[VD]))] = allVertices.mapValues{ case (intv, attr) => (intv, Set(attr))}
      val verts2: RDD[(VertexId, (Interval, Set[VD]))] = grp2.allVertices.mapValues{ case (intv, attr) => (intv, Set(attr))}
      val edg1: RDD[((VertexId,VertexId), (Interval,Set[ED]))] = allEdges.mapValues{ case (intv, attr) => (intv, Set(attr))}
      val edg2: RDD[((VertexId,VertexId), (Interval,Set[ED]))] = grp2.allEdges.mapValues{ case (intv, attr) => (intv, Set(attr))}

      new SnapshotGraphParallel(newIntvs, verts1.union(verts2), edg1.union(edg2), newGraphs, Set(defaultValue), storageLevel, coalesced && grp2.coalesced)
    }
  }

  override def intersection(other: TGraph[VD, ED]): SnapshotGraphParallel[Set[VD], Set[ED]] = {
    var grp2: SnapshotGraphParallel[VD, ED] = other match {
      case grph: SnapshotGraphParallel[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (span.intersects(grp2.span)) {
      if (graphs.size < 1) computeGraphs()
      if (grp2.graphs.size < 1) grp2.computeGraphs()
      //similar to union, there are two ways to do this
      //by graphs or by tuples with join
      //this is by graphs

      //compute new intervals
      val newIntvs: RDD[Interval] = TempGraphOps.intervalIntersect(intervals, grp2.intervals)

      var ii: Integer = 0
      var jj: Integer = 0
      val empty: Interval = new Interval(LocalDate.MAX, LocalDate.MAX)
      val intervalsZipped = intervals.zipWithIndex.map(_.swap)
      val intervals2Zipped = grp2.intervals.zipWithIndex.map(_.swap)

      val head = newIntvs.min

      while (!intervalsZipped.lookup(ii.toLong).lift(0).getOrElse(empty).intersects(head)) ii = ii + 1
      while (!intervals2Zipped.lookup(jj.toLong).lift(0).getOrElse(empty).intersects(head)) jj = jj + 1

      //TODO: get rid of collect if possible
      val newGraphs: ParSeq[Graph[Set[VD], Set[ED]]] = newIntvs.collect.map { intv =>
        val iith = intervalsZipped.lookup(ii.toLong).lift(0).getOrElse(empty)
        val jjth = intervals2Zipped.lookup(jj.toLong).lift(0).getOrElse(empty)
        if (iith.intersects(intv) && jjth.intersects(intv)) {
          //TODO: an innerJoin on edges would be more efficient
          //but it requires the exact same number of partitions and partition strategy
          //see whether repartitioning and innerJoin is better
          val ret = Graph(graphs(ii).vertices.join(grp2.graphs(jj).vertices).mapValues( attr => Set(attr._1, attr._2)), 
            graphs(ii).edges.map(e => ((e.srcId, e.dstId), e.attr)).join(grp2.graphs(jj).edges.map(e => ((e.srcId, e.dstId), e.attr))).map{ case (k, v) => Edge(k._1, k._2, Set(v._1, v._2))}, Set(defaultValue), storageLevel, storageLevel)
          if (iith.end == intv.end)
            ii = ii+1
          if (jjth.end == intv.end)
            jj = jj+1
          ret
        } else { //should never get here
          throw new SparkException("bug in intersection")
        }
      }.par

      SnapshotGraphParallel.fromGraphs(newIntvs, newGraphs, Set(defaultValue), storageLevel)

    } else {
      SnapshotGraphParallel.emptyGraph(Set(defaultValue))
    }
  }

  override def pregel[A: ClassTag]
     (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
       sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): SnapshotGraphParallel[VD, ED] = {
    if (graphs.size < 1) computeGraphs()
    SnapshotGraphParallel.fromGraphs(intervals, graphs.map(x => Pregel(x, initialMsg,
      maxIterations, activeDirection)(vprog, sendMsg, mergeMsg)), defaultValue, storageLevel)
  }

  override def degree: RDD[(VertexId, (Interval, Int))] = {
    if (!allEdges.isEmpty) {
      if (graphs.size < 1) computeGraphs()
      //TODO: get rid of collect if possible
      val total = graphs.zip(intervals.collect)
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
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): SnapshotGraphParallel[(VD,Double), ED] = {
    if (graphs.size < 1) computeGraphs()

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

    val pranks = graphs.map(safePagerank)
    //need to join vertices
    val newGraphs = graphs.zip(pranks).map{ case (a,b) =>
      a.outerJoinVertices(b.vertices)((vid, attr, deg) => (attr, deg.getOrElse(0.0)))
    }
    SnapshotGraphParallel.fromGraphs(intervals, newGraphs, (defaultValue,0.0), storageLevel)

  }

  override def connectedComponents(): SnapshotGraphParallel[(VD,VertexId), ED] = {
    if (graphs.size < 1) computeGraphs()

    val safeConnectedComponents = (grp: Graph[VD, ED]) => {
      if (grp.vertices.isEmpty) {
        Graph[VertexId, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        grp.connectedComponents()
      }
    }
    val cons = graphs.map(safeConnectedComponents)
    val newGraphs = graphs.zip(cons).map{ case (a,b) =>
      a.outerJoinVertices(b.vertices)((vid, attr, con) => (attr, con.getOrElse(-1L)))
    }
    SnapshotGraphParallel.fromGraphs(intervals, newGraphs, (defaultValue, -1), storageLevel)

  }

  override def shortestPaths(uni: Boolean, landmarks: Seq[VertexId]): SnapshotGraphParallel[(VD,Map[VertexId, Int]), ED] = {
    if (graphs.size < 1) computeGraphs()

    val safeShortestPaths = (grp: Graph[VD, ED]) => {
      if (grp.vertices.isEmpty) {
        Graph[Map[VertexId, Int], ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        if (!uni)
          ShortestPathsXT.run(grp, landmarks)
        else
          ShortestPaths.run(grp, landmarks).mapVertices((vid, vattr) => mapAsJavaMap(vattr))
      }
    }
    val spaths = graphs.map(safeShortestPaths)
    val defV = new Long2IntOpenHashMap().asInstanceOf[Map[VertexId,Int]]
    val newGraphs = graphs.zip(spaths).map{ case (a,b) =>
      a.outerJoinVertices(b.vertices)((vid, attr, pa) => (attr, pa.getOrElse(defV)))
    }
    SnapshotGraphParallel.fromGraphs(intervals, newGraphs, (defaultValue, defV), storageLevel)

  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    //FIXME: this seems an overkill to compute graphs just to get the number of partitions
    if (graphs.size < 1) computeGraphs()
    graphs.filterNot(_.edges.isEmpty).map(_.edges.getNumPartitions).reduce(_ + _)
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

  override def partitionBy(tgp: TGraphPartitioning): SnapshotGraphParallel[VD, ED] = {
    if (tgp.pst != PartitionStrategyType.None) {
      //not changing the intervals, only the graphs at their indices
      //each partition strategy for SG needs information about the graph

      partitioning = tgp
      //we use lazy partitioning. i.e., we partition when we compute graphs
      if (graphs.size > 0) {
        //use that strategy to partition each of the snapshots
        new SnapshotGraphParallel(intervals, allVertices, allEdges, graphs.zipWithIndex.map { case (g,i) =>
          val numParts: Int = if (tgp.parts > 0) tgp.parts else g.edges.getNumPartitions
          g.partitionBy(PartitionStrategies.makeStrategy(tgp.pst, i, graphs.size, tgp.runs), numParts)
        }, defaultValue, storageLevel, coalesced)
      } else this
    } else
      this
  }

  override protected def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false): SnapshotGraphParallel[V, E] = {
    SnapshotGraphParallel.fromRDDs(verts, edgs, defVal, storLevel, coal)
  }

  protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): SnapshotGraphParallel[V, E] = SnapshotGraphParallel.emptyGraph(defVal)

  protected def computeGraphs(): Unit = {
    //compute the graphs. due to spark lazy evaluation,
    //if these graphs are not needed, they aren't actually materialized
    //TODO: get rid of collect if possible
    val intervalsc = intervals.collect
    val redFactor = intervalsc.size
    graphs = intervalsc.map( p =>
      Graph(allVertices.filter(v => v._2._1.intersects(p)).coalesce(math.max(1, allVertices.getNumPartitions/redFactor))(null).map(v => (v._1, v._2._2)),
        allEdges.filter(e => e._2._1.intersects(p)).coalesce(math.max(1, allEdges.getNumPartitions/redFactor))(null).map(e => Edge(e._1._1, e._1._2, e._2._2)),
        defaultValue, storageLevel, storageLevel)
    ).par

    if (partitioning.pst != PartitionStrategyType.None) {
      graphs = graphs.zipWithIndex.map { case (g,i) =>
        val numParts: Int = if (partitioning.parts > 0) partitioning.parts else g.edges.getNumPartitions
        g.partitionBy(PartitionStrategies.makeStrategy(partitioning.pst, i, graphs.size, partitioning.runs), numParts)
      }
    }
  }
  
}

object SnapshotGraphParallel extends Serializable {
  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): SnapshotGraphParallel[V, E] = {
    val cverts = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(verts) else verts
    val cedges = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(edgs) else edgs
    val coal = coalesced | ProgramContext.eagerCoalesce

    val intervals = TGraphNoSchema.computeIntervals(cverts, cedges)

    //because we use "lazy" evaluation, we don't compute the graphs until we need them
    new SnapshotGraphParallel(intervals, cverts, cedges, ParSeq[Graph[V,E]](), defVal, storLevel, coal)
  }

  def fromGraphs[V: ClassTag, E: ClassTag](intervals: RDD[Interval], graphs: ParSeq[Graph[V, E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY): SnapshotGraphParallel[V, E] = {
    //TODO: get rid of collect if possible
    val intervalsc = intervals.collect

    val verts: RDD[(VertexId, (Interval, V))] = graphs.zip(intervalsc).map{ case (g,i) => g.vertices.map{ case (vid, attr) => (vid, (i, attr))}}.reduce((a, b) => a union b)
    val edges: RDD[((VertexId, VertexId), (Interval, E))] = graphs.zip(intervalsc).map{ case (g,i) => g.edges.map(e => ((e.srcId, e.dstId), (i, e.attr)))}.reduce((a, b) => a union b)

    if (ProgramContext.eagerCoalesce)
      fromRDDs(verts, edges, defVal, storLevel, false)
    else
      new SnapshotGraphParallel(intervals, verts, edges, graphs, defVal, storLevel, false)
  }

  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V):SnapshotGraphParallel[V, E] = new SnapshotGraphParallel(ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, ParSeq[Graph[V,E]](), defVal, coal = true)
 
}
