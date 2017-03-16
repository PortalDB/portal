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
import org.apache.commons.lang.NotImplementedException
import src.main.scala.edu.drexel.cs.dbgroup.temporalgraph.TEdge

class SnapshotGraphParallel[VD: ClassTag, ED: ClassTag](intvs: Array[Interval], grphs: ParSeq[Graph[VD,(EdgeId,ED)]], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraphNoSchema[VD, ED](defValue, storLevel, coal) {

  protected var graphs: ParSeq[Graph[VD, (EdgeId,ED)]] = grphs
  val intervals: Array[Interval] = intvs

  //TODO: we should enforce the integrity constraint
  //by removing edges which connect nonexisting vertices at some time t
  //or throw an exception upon construction

  override def size(): Interval = span
  lazy val span: Interval = if (intervals.size > 0) Interval(intervals.head.start, intervals.last.end) else Interval(LocalDate.now, LocalDate.now)

  override def materialize() = {
    graphs.foreach { x =>
      x.vertices.count
      x.edges.count
    }
  }

  override def vertices: RDD[(VertexId, (Interval, VD))] = coalescedVertices

  lazy val verticesRaw: RDD[(VertexId, (Interval, VD))] = {
    if (graphs.size > 0)
      graphs.zip(intervals).map{ case (g,i) => g.vertices.map{ case (vid, attr) => (vid, (i, attr))}}.reduce((a, b) => a union b)
    else
      ProgramContext.sc.emptyRDD[(VertexId, (Interval, VD))]
  }

  private lazy val coalescedVertices = TGraphNoSchema.coalesce(verticesRaw)

  override def edges: RDD[TEdge[ED]] = coalescedEdges

  lazy val edgesRaw: RDD[TEdge[ED]] = {
    if (graphs.size > 0)
      graphs.zip(intervals).map{ case (g,i) => g.edges.map(e => ((e.attr._1, e.srcId, e.dstId), (i, e.attr._2)))}.reduce((a, b) => a union b).map(e => TEdge.apply(e._1,e._2))
    else
      ProgramContext.sc.emptyRDD[(TEdge[ED])]
  }

  private lazy val coalescedEdges = TGraphNoSchema.coalesce(edgesRaw.map(e => e.toPaired())).map(e => TEdge.apply(e._1,e._2))

  override def getTemporalSequence: RDD[Interval] = ProgramContext.sc.parallelize(coalescedIntervals)

  private lazy val coalescedIntervals = {
    if (coalesced)
      intervals
    else
      coalesce().intervals
  }

  override def getSnapshot(time: LocalDate): Graph[VD,(EdgeId,ED)] = {
    val index = intervals.indexWhere(ii => ii.contains(time))
    if (index >= 0) {
      graphs(index)
    } else
      Graph[VD,(EdgeId,ED)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
  }

  override def coalesce(): SnapshotGraphParallel[VD, ED] = {
    if (coalesced)
      this
    else {
      //this is a very expensive operation; use sparingly
      val res = graphs.zip(intervals).foldLeft(List[(Graph[VD,(EdgeId,ED)],Interval)]()) { (r,c) => r match {
        case head :: tail => {
          //VZM: there is a bug in graphx which allows to compile
          //subract operation on the graph edges but dies runtime
          val e1 = head._1.edges.map(e => ((e.srcId, e.dstId), e.attr))
          val e2 = c._1.edges.map(e => ((e.srcId, e.dstId), e.attr))
          if (e1.subtract(e2).isEmpty &&
              e2.subtract(e1).isEmpty &&
              head._1.vertices.subtract(c._1.vertices).isEmpty &&
              c._1.vertices.subtract(head._1.vertices).isEmpty)
            (head._1, Interval(head._2.start, c._2.end)) :: tail
          else c :: r
        }
        case Nil => 
          if (c._1.vertices.isEmpty) List() else List(c)
      }}.dropWhile(intv => intv._1.vertices.isEmpty)
        .reverse

      new SnapshotGraphParallel(res.map(_._2).toArray, res.map(_._1).par, defaultValue, storageLevel, true)
    }
  }

  /** Algebraic operations */

  override def slice(bound: Interval): SnapshotGraphParallel[VD, ED] = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this
    if (!span.intersects(bound)) {
      return SnapshotGraphParallel.emptyGraph[VD,ED](defaultValue)
    }

    val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
    val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end
    val selectBound:Interval = Interval(startBound, endBound)
    //compute indices of start and stop
    val selectStart:Int = intervals.indexWhere(ii => ii.intersects(selectBound))
    val selectStop:Int = intervals.lastIndexWhere(ii => ii.intersects(selectBound))


    val zipped = intervals.zipWithIndex.filter(intv => intv._1.intersects(selectBound))

    val newIntvs: Array[Interval] = intervals.slice(selectStart, selectStop+1).map(intv => if (intv.start.isBefore(startBound) || intv.end.isAfter(endBound)) intv.intersection(selectBound).get else intv)

    new SnapshotGraphParallel(newIntvs, graphs.slice(selectStart, selectStop+1), defaultValue, storageLevel, coalesced)

  }

  override def vsubgraph( vpred: (VertexId, VD,Interval) => Boolean): SnapshotGraphParallel[VD,ED] = {
    //Todo: Implement this( maybe we can use two level of filtering)
    throw  new NotImplementedError()
    //new SnapshotGraphParallel(intervals, graphs.map(g => g.subgraph(vpred=vpred, defaultValue, storageLevel, false)
  }

  override def esubgraph(epred: (EdgeTriplet[VD,ED],Interval ) => Boolean,tripletFields: TripletFields = TripletFields.All): SnapshotGraphParallel[VD,ED] = {
    //Todo: Implement this( maybe we can use two level of filtering)
    throw  new NotImplementedError()
    //new SnapshotGraphParallel(intervals, graphs.map(g => g.subgraph(epred = et => epred((et.srcId, et.dstId), et.attr))), defaultValue, storageLevel, false)
  }

  //expects coalesced input
  override protected def aggregateByChange(c: ChangeSpec, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): SnapshotGraphParallel[VD, ED] = {
    val size: Integer = c.num
    val intervalsc = intervals
    var groups: ParSeq[List[(Graph[VD, (EdgeId,ED)], Interval)]] = if (size > 1)
      graphs.zip(intervalsc).foldLeft(List[List[(Graph[VD, (EdgeId,ED)], Interval)]](), 0) { (r, c) => r match {
        case (head :: tail, num) =>
          if (num < size)  ( (c :: head) :: tail , num + 1 )
          else             ( List(c) :: head :: tail , 1 )
        case (Nil, num) => (List(List(c)), 1)
      }
      }._1.foldLeft(List[List[(Graph[VD, (EdgeId,ED)], Interval)]]())( (r,c) => c.reverse :: r).par
    else
      graphs.zip(intervalsc).map(g => List(g))

    //for each group, reduce into vertices and edges
    //compute new value, filter by quantification
    val reduced: ParSeq[(RDD[(VertexId, (VD, Double))], RDD[((EdgeId, VertexId, VertexId),(ED, Double))])] = groups.map{group =>
      val intv = Interval(group.head._2.start, group.last._2.end)
      group.map{ case (g,ii) =>
        //map each vertex into its new key
        (g.vertices.map{ case (vid, vattr) => (vid, (vattr, ii.ratio(intv)))},
          g.triplets.map{ e => ((e.attr._1, e.srcId, e.dstId), (e.attr._2, ii.ratio(intv)))})}
        .reduce((a: (RDD[(VertexId, (VD, Double))], RDD[((EdgeId, VertexId, VertexId), (ED, Double))]), b: (RDD[(VertexId, (VD, Double))], RDD[((EdgeId, VertexId, VertexId),(ED, Double))])) => (a._1.union(b._1), a._2.union(b._2)))
      //reduce by key with aggregate functions
    }.map{ case (vs, es) =>
      (vs.reduceByKey((a,b) => (vAggFunc(a._1, b._1), a._2 + b._2))
        .filter(v => vquant.keep(v._2._2)),
        es.reduceByKey((a,b) => (eAggFunc(a._1, b._1), a._2 + b._2))
          .filter(e => equant.keep(e._2._2)))
    }

    //now we can create new graphs
    //to enforce constraint on edges, subgraph vertices that have default attribute value
    val vp = (vid: VertexId, attr: VD) => { val tt: VD = new Array[VD](1)(0); attr != tt}
    val newGraphs: ParSeq[Graph[VD, (EdgeId,ED)]] = reduced.map { case (vs, es) =>
      val g = Graph[VD,(EdgeId,ED)](vs.mapValues(v => v._1), es.map(e => Edge(e._1._2, e._1._3, (e._1._1,e._2._1))), null.asInstanceOf[VD], storageLevel, storageLevel)
      if (vquant.threshold <= equant.threshold) g else g.subgraph(vpred = vp)
    }

    val newIntervals: Array[Interval] = intervals.grouped(size).map(grp => Interval(grp(0).start, grp.last.end)).toArray
    new SnapshotGraphParallel(newIntervals, newGraphs, defaultValue, storageLevel, false)

  }

  override def aggregateByTime(c: TimeSpec, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): SnapshotGraphParallel[VD, ED] = {
    //we need groups of graphs
    //each graph may map into 1 or more new intervals
    val start = span.start
    val groups: ParSeq[(Interval, List[(Graph[VD, (EdgeId,ED)], Interval)])] = graphs.zip(intervals).flatMap{ case (g,ii) => ii.split(c.res, start).map(x => (x._2, (g, ii)))}.toList.groupBy(_._1).toList.map(x => (x._1, x._2.map(y => y._2))).sortBy(x => x._1).par
    //for each group, reduce into vertices and edges
    //compute new value, filter by quantification
    val reduced: ParSeq[(RDD[(VertexId, (VD, Double))], RDD[((EdgeId, VertexId, VertexId),(ED, Double))])] = groups.map{ case (intv, group) =>
      group.map{ case (g,ii) =>
        //map each vertex into its new key
        (g.vertices.map{ case (vid, vattr) => (vid, (vattr, ii.ratio(intv)))},
          g.triplets.map{ e => ((e.attr._1, e.srcId,e.dstId), (e.attr._2, ii.ratio(intv)))})}
        .reduce((a: (RDD[(VertexId, (VD, Double))], RDD[((EdgeId, VertexId, VertexId), (ED, Double))]), b: (RDD[(VertexId, (VD, Double))], RDD[((EdgeId, VertexId, VertexId),(ED, Double))])) => (a._1.union(b._1), a._2.union(b._2)))
      //reduce by key with aggregate functions
    }.map{ case (vs, es) =>
      (vs.reduceByKey((a,b) => (vAggFunc(a._1, b._1), a._2 + b._2))
        .filter(v => vquant.keep(v._2._2)),
        es.reduceByKey((a,b) => (eAggFunc(a._1, b._1), a._2 + b._2))
          .filter(e => equant.keep(e._2._2))
        )
    }
    //now we can create new graphs
    //to enforce constraint on edges, subgraph vertices that have default attribute value
    val vp = (vid: VertexId, attr: VD) => { val tt: VD = new Array[VD](1)(0); attr != tt}
    val newGraphs: ParSeq[Graph[VD, (EdgeId,ED)]] = reduced.map { case (vs, es) =>
      val g = Graph(vs.mapValues(v => v._1), es.map(e => Edge(e._1._2, e._1._3, (e._1._1,e._2._1))), null.asInstanceOf[VD], storageLevel, storageLevel)
      if (vquant.threshold <= equant.threshold) g else g.subgraph(epred = et => true, vpred = vp)
    }

    val newIntervals = span.split(c.res, start).map(_._2).reverse.toArray
    new SnapshotGraphParallel(newIntervals, newGraphs, defaultValue, storageLevel, false)

  }

  override def createAttributeNodes(vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED)(vgroupby: (VertexId, VD) => VertexId ): SnapshotGraphParallel[VD, ED]={
    val reduced: ParSeq[(RDD[(VertexId, VD)], RDD[((EdgeId, VertexId, VertexId),ED)])] = graphs.map(g =>
        (g.vertices.map( v => (vgroupby(v._1, v._2), v._2)).reduceByKey((a,b) => vAggFunc(a,b)),
          g.triplets.map{ e => ((e.attr._1, vgroupby(e.srcId, e.srcAttr), vgroupby(e.dstId, e.dstAttr)), e.attr._2)}.reduceByKey((a,b) => eAggFunc(a,b))))

    //now we can create new graphs
    val newGraphs: ParSeq[Graph[VD,(EdgeId,ED)]] =   reduced.map { case (vs, es) =>
      Graph[VD, (EdgeId,ED)](vs, es.map(e => Edge(e._1._2, e._1._3, (e._1._1,e._2))), null.asInstanceOf[VD], storageLevel, storageLevel)
    }
    new SnapshotGraphParallel(intervals, newGraphs, defaultValue, storageLevel, false)
  }

  override def vmap[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2, defVal: VD2)(implicit eq: VD =:= VD2 = null): SnapshotGraphParallel[VD2, ED] = {
    new SnapshotGraphParallel(intervals, graphs.zip(intervals).map(g => g._1.mapVertices((vid, attr) => map(vid, g._2, attr))), defVal, storageLevel, false)
  }

  override def emap[ED2: ClassTag](map: (Interval, Edge[(EdgeId,ED)]) => (EdgeId,ED2)): SnapshotGraphParallel[VD, ED2] = {
    new SnapshotGraphParallel(intervals, graphs.zip(intervals).map(g => g._1.mapEdges(e => map(g._2, e))), defaultValue, storageLevel, false)
  }

  override def union(other: TGraphNoSchema[VD, ED],vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): SnapshotGraphParallel[VD, ED] = {

    var grp2: SnapshotGraphParallel[VD, ED] = other match {
      case grph: SnapshotGraphParallel[VD, ED] => grph
      case _ => throw new ClassCastException
    }
    if (span.intersects(grp2.span)) {
      //compute new intervals
      implicit val ord = TempGraphOps.dateOrdering
      val newIntvs: Array[Interval] = intervals.flatMap(ii => Seq(ii.start, ii.end)).union(grp2.intervals.flatMap(ii => Seq(ii.start, ii.end))).distinct.sortBy(c => c).sliding(2).map(x => Interval(x(0), x(1))).toArray

      var ii: Integer = 0
      var jj: Integer = 0
      val empty: Interval = Interval.empty
      implicit val ord2: Ordering[Interval] = Ordering.fromLessThan((a,b) => a.start.isBefore(b.start))

      val newGraphs: ParSeq[Graph[VD, (EdgeId,ED)]] = newIntvs.map { intv =>
        val iith = intervals.lift(ii).getOrElse(empty)
        val jjth = grp2.intervals.lift(jj).getOrElse(empty)
        if (iith.intersects(intv) && jjth.intersects(intv)) {
          val ret: Graph[VD, (EdgeId,ED)] = Graph(graphs(ii).vertices.union(grp2.graphs(jj).vertices).reduceByKey(vFunc),
            graphs(ii).edges.map(e => ((e.attr._1, e.srcId, e.dstId), e.attr._2)).union(grp2.graphs(jj).edges.map(e => ((e.attr._1,e.srcId, e.dstId), e.attr._2))).reduceByKey(eFunc).map( e => Edge( e._1._2,e._1._3,(e._1._1,e._2))),
            defaultValue, storageLevel, storageLevel)
          if (iith.end == intv.end)
            ii = ii+1
          if (jjth.end == intv.end)
            jj = jj+1
          ret
        } else if (iith.intersects(intv)) {
          if (iith.end == intv.end)
            ii = ii+1
          graphs(ii-1)
        } else if (jjth.intersects(intv)) {
          if (jjth.end == intv.end)
            jj = jj+1
          grp2.graphs(jj-1)
        } else { //should never get here
          throw new SparkException("bug in union")
        }
      }.par

      new SnapshotGraphParallel(newIntvs, newGraphs, defaultValue, storageLevel, false)
    } else if (span.end == grp2.span.start || span.start == grp2.span.end) {
      //if the two spans are one right after another but do not intersect
      //then we just put them together
      val newIntvs = intervals.union(grp2.intervals).sortBy(c => c)
      //need to update values for all vertices and edges
      val newGraphs = if (span.start.isBefore(grp2.span.start)) graphs ++ grp2.graphs else grp2.graphs ++ graphs
      new SnapshotGraphParallel(newIntvs, newGraphs, defaultValue, storageLevel, false)
    } else {
      //if there is no temporal intersection, then we can just add them together
      //no need to worry about coalesce or constraint on E; all still holds
      val newIntvs = intervals.union(grp2.intervals).union(Seq(Interval(span.end, grp2.span.start))).sortBy(c => c)
      val newGraphs = if (span.start.isBefore(grp2.span.start)) graphs ++ Seq(Graph[VD, (EdgeId,ED)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)) ++ grp2.graphs else grp2.graphs ++ Seq(Graph[VD, (EdgeId,ED)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)) ++ graphs
      new SnapshotGraphParallel(newIntvs, newGraphs, defaultValue, storageLevel, coalesced && grp2.coalesced)
    }
  }

  override def difference(other: TGraphNoSchema[VD, ED]): SnapshotGraphParallel[VD, ED] = {
    val grp2: SnapshotGraphParallel[VD, ED] = other match {
      case grph: SnapshotGraphParallel[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (span.intersects(grp2.span)) {
      //compute new intervals
      implicit val ord = TempGraphOps.dateOrdering
      val newIntvs: Array[Interval] = intervals.flatMap(ii => Seq(ii.start, ii.end)).union(grp2.intervals.flatMap(ii => Seq(ii.start, ii.end)).filter(ii => span.contains(ii))).distinct.sortBy(c => c).sliding(2).map(x => Interval(x(0), x(1))).toArray

      var ii: Integer = 0
      var jj: Integer = 0
      val empty: Interval = Interval.empty

      val head = newIntvs.min
       val newGraphs: ParSeq[Graph[VD, (EdgeId,ED)]] =
        newIntvs.map { intv =>
        val iith = intervals.lift(ii).getOrElse(empty)
        val jjth = grp2.intervals.lift(jj).getOrElse(empty)
        if (iith.intersects(intv) && jjth.intersects(intv)) {
          val temp = graphs(ii).outerJoinVertices(grp2.graphs(jj).vertices)((id, vd, optmatch) =>
            optmatch match {
              case Some(id) => (vd, 1)
              case None => (vd, 0)
            }
          ).subgraph(vpred = (id, attr) => attr._2 == 0)

        val ret: Graph[VD, (EdgeId,ED)] = Graph(temp.vertices.map(in=>(in._1,in._2._1)), temp.edges, defaultValue, storageLevel, storageLevel)
          if (iith.end == intv.end)
            ii = ii + 1
          if (jjth.end == intv.end)
            jj = jj + 1
          ret
        }else if (iith.intersects(intv)) {
            if (iith.end == intv.end)
              ii = ii+1
            graphs(ii-1).mapVertices((vid,attr) => (attr)).mapEdges(e => (e.attr))
          }else { //should never get here
          throw new SparkException("bug in difference")
        }
      }.par
      new SnapshotGraphParallel(newIntvs, newGraphs, defaultValue, storageLevel, false)
    } else {
        this
    }
  }

  override def intersection(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): SnapshotGraphParallel[VD, ED] = {
    var grp2: SnapshotGraphParallel[VD, ED] = other match {
      case grph: SnapshotGraphParallel[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    implicit val ord = TempGraphOps.dateOrdering

    if (span.intersects(grp2.span)) {
      //compute new intervals
      val st = TempGraphOps.maxDate(intervals.head.start, grp2.intervals.head.start)
      val en = TempGraphOps.minDate(intervals.last.end, grp2.intervals.last.end)
      val in = Interval(st, en)
      val newIntvs: Array[Interval] = intervals.map(ii => ii.start).filter(ii => in.contains(ii)).union(grp2.intervals.map(ii => ii.start).filter(ii => in.contains(ii))).union(Seq(en)).distinct.sortBy(c => c).sliding(2).map(x => Interval(x(0), x(1))).toArray

      var ii: Integer = 0
      var jj: Integer = 0
      val empty: Interval = Interval.empty

      val head = newIntvs.min

      while (!intervals.lift(ii).getOrElse(empty).intersects(head)) ii = ii + 1
      while (!grp2.intervals.lift(jj).getOrElse(empty).intersects(head)) jj = jj + 1

      val newGraphs: ParSeq[Graph[VD, (EdgeId,ED)]] = newIntvs.map { intv =>
        val iith = intervals.lift(ii).getOrElse(empty)
        val jjth = grp2.intervals.lift(jj).getOrElse(empty)
        if (iith.intersects(intv) && jjth.intersects(intv)) {
          //TODO: an innerJoin on edges would be more efficient
          //but it requires the exact same number of partitions and partition strategy
          //see whether repartitioning and innerJoin is better

          val ret = Graph(graphs(ii).vertices.join(grp2.graphs(jj).vertices).mapValues{ case (a,b) => vFunc(a,b)},
            graphs(ii).edges.map(e => ((e.attr._1, e.srcId, e.dstId), e.attr._2)).join(grp2.graphs(jj).edges.map(e => ((e.attr._1,e.srcId, e.dstId), e.attr._2))).map{ case (k, v) => Edge(k._2, k._3, (k._1,eFunc(v._1,v._2)))},
            defaultValue, storageLevel, storageLevel)
          if (iith.end == intv.end)
            ii = ii+1
          if (jjth.end == intv.end)
            jj = jj+1
          ret
        } else { //should never get here
          throw new SparkException("bug in intersection")
        }
      }.par

      new SnapshotGraphParallel(newIntvs, newGraphs, defaultValue, storageLevel, false)

    } else {
      SnapshotGraphParallel.emptyGraph(defaultValue)
    }
  }

  /** Analytics */

  override def pregel[A: ClassTag]
     (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
       sendMsg: EdgeTriplet[VD, (EdgeId,ED)] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): SnapshotGraphParallel[VD, ED] = {
    new SnapshotGraphParallel(intervals, graphs.map(x => Pregel(x, initialMsg,
      maxIterations, activeDirection)(vprog, sendMsg, mergeMsg)), defaultValue, storageLevel, false)
  }

  override def degree: RDD[(VertexId, (Interval, Int))] = {
    //TODO: get rid of collect if possible
    val total = graphs.zip(intervals)
      .filterNot(x => x._1.edges.isEmpty)
      .map(x => x._1.degrees.mapValues(deg => (x._2,deg)))
    if (total.size > 0)
      TGraphNoSchema.coalesce(total.reduce((x: RDD[(VertexId, (Interval, Int))] ,y: RDD[(VertexId, (Interval, Int))]) => x union y))
    else {
      ProgramContext.sc.emptyRDD
    }
  }

  //run PageRank on each contained snapshot
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): SnapshotGraphParallel[(VD,Double), ED] = {
    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    val vertexProgram = (id: VertexId, attr: (Double, Double), msgSum: Double) => {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }

    val sendMessage = (edge: EdgeTriplet[(Double, Double), (Double, Double)]) => {
      if (edge.srcAttr._2 > tol && edge.dstAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr._1),(edge.srcId, edge.dstAttr._2 * edge.attr._2))
      } else if (edge.srcAttr._2 > tol) { //means dstAttr is not >
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr._1))
      } else if (edge.dstAttr._2 > tol) { //means srcAttr is not >
        Iterator((edge.srcId, edge.dstAttr._2 * edge.attr._2))
      } else {
        Iterator.empty
      }
    }

    val messageCombiner = (a: Double, b: Double) => a + b

    val safePagerank = (grp: Graph[VD, (EdgeId,ED)]) => {
      if (grp.edges.isEmpty) {
        Graph[Double, Double](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        if (!uni) {
          // Initialize the pagerankGraph with each edge attribute
          // having weight 1/degree and each vertex with attribute 1.0.
          val pagerankGraph: Graph[(Double, Double), (Double,Double)] = grp
          // Associate the degree with each vertex
            .outerJoinVertices(grp.degrees) {
            (vid, vdata, deg) => deg.getOrElse(0)
          }
          // Set the weight on the edges based on the degree
            .mapTriplets( e => (1.0 / e.srcAttr, 1.0 / e.dstAttr) )
          // Set the vertex attributes to (initalPR, delta = 0)
            .mapVertices( (id, attr) => (0.0, 0.0) )
            .cache()

          // The initial message received by all vertices in PageRank
          val initialMessage = resetProb / (1.0 - resetProb)

          // Execute a dynamic version of Pregel.
          Pregel(pagerankGraph, initialMessage, numIter,
            activeDirection = EdgeDirection.Either)(
            vertexProgram, sendMessage, messageCombiner)
            .mapTriplets(e => e.attr._1) //I don't think it matters which we pick
            .mapVertices((vid, attr) => attr._1)
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
    new SnapshotGraphParallel(intervals, newGraphs, (defaultValue,0.0), storageLevel, coalesced)

  }

  override def connectedComponents(): SnapshotGraphParallel[(VD,VertexId), ED] = {
    val safeConnectedComponents = (grp: Graph[VD, (EdgeId,ED)]) => {
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
    new SnapshotGraphParallel(intervals, newGraphs, (defaultValue, -1), storageLevel, coalesced)

  }

  override def shortestPaths(uni: Boolean, landmarks: Seq[VertexId]): SnapshotGraphParallel[(VD,Map[VertexId, Int]), ED] = {
    val safeShortestPaths = (grp: Graph[VD, (EdgeId,ED)]) => {
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
    new SnapshotGraphParallel(intervals, newGraphs, (defaultValue, defV), storageLevel, coalesced)

  }

  override def aggregateMessages[A: ClassTag](sendMsg: EdgeTriplet[VD, (EdgeId,ED)] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A, defVal: A, tripletFields: TripletFields = TripletFields.All): SnapshotGraphParallel[(VD, A), ED] = {

    val send = (ctx: EdgeContext[VD, (EdgeId,ED), A]) => {
      sendMsg(ctx.toEdgeTriplet).foreach { kv =>
        if (kv._1 == ctx.srcId)
          ctx.sendToSrc(kv._2)
        else if (kv._1 == ctx.dstId)
          ctx.sendToDst(kv._2)
        else
          throw new IllegalArgumentException("trying to send message to neither the triplet source or destination")
      }
    }

    val newGraphs = graphs.zip(graphs.map(x => x.aggregateMessages(send, mergeMsg, tripletFields))).map{ case (a,b) =>
      a.outerJoinVertices(b)((vid, attr, agg) => (attr, agg.getOrElse(defVal)))
    }
    new SnapshotGraphParallel(intervals, newGraphs, (defaultValue, defVal), storageLevel, coalesced)
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    graphs.filterNot(_.edges.isEmpty).map(_.edges.getNumPartitions).reduce(_ + _)
  }

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): SnapshotGraphParallel[VD, ED] = {
    //persist each graph
    //this will throw an exception if the graphs are already persisted
    //with a different storage level
    graphs.map(g => g.persist(newLevel))
    this
  }

  override def unpersist(blocking: Boolean = true): SnapshotGraphParallel[VD, ED] = {
    graphs.map(_.unpersist(blocking))
    this
  }

  override def partitionBy(tgp: TGraphPartitioning): SnapshotGraphParallel[VD, ED] = {
    if (tgp.pst != PartitionStrategyType.None) {
      //not changing the intervals, only the graphs at their indices
      //each partition strategy for SG needs information about the graph

      //use that strategy to partition each of the snapshots
      new SnapshotGraphParallel(intervals, graphs.zipWithIndex.map { case (g,i) =>
        val numParts: Int = if (tgp.parts > 0) tgp.parts else g.edges.getNumPartitions
        g.partitionBy(PartitionStrategies.makeStrategy(tgp.pst, i, graphs.size, tgp.runs), numParts)
      }, defaultValue, storageLevel, coalesced)
    } else
      this
  }

  override protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): SnapshotGraphParallel[V, E] = SnapshotGraphParallel.emptyGraph(defVal)

}

object SnapshotGraphParallel extends Serializable {
  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[TEdge[E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): SnapshotGraphParallel[V, E] = {
    val cverts = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(verts) else verts
    val cedges: RDD[TEdge[E]] = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(edgs.map{e => e.toPaired()}).map{e => TEdge.apply(e._1,e._2)} else edgs
    val coal = coalesced | ProgramContext.eagerCoalesce

    val intervals = TGraphNoSchema.computeIntervals(cverts, cedges).collect
    //TODO: the performance strongly depends on the number of partitions
    //need a good way to compute a good number
    val graphs = intervals.map( p =>
      Graph(cverts.filter(v => v._2._1.intersects(p)).map(v => (v._1, v._2._2)),
        cedges.filter(e => e.interval.intersects(p)).map(e => Edge(e.srcId, e.dstId, (e.eId,e.attr))),
        defVal, storLevel, storLevel)
    ).par

    new SnapshotGraphParallel(intervals, graphs, defVal, storLevel, coal)
  }

  def fromDataFrames[V: ClassTag, E: ClassTag](verts: org.apache.spark.sql.DataFrame, edgs: org.apache.spark.sql.DataFrame, defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): SnapshotGraphParallel[V, E] = {
    //we can filter directly on dataframe which will do filter pushdown for faster load
    //TODO: check for eagerCoalesce and coalesce if needed

    val intervals = verts.select(verts("estart"), verts("eend")).rdd.flatMap(r => Seq(r.getLong(0), r.getLong(1))).union(edgs.select(edgs("estart"), edgs("eend")).rdd.flatMap(r => Seq(r.getLong(0), r.getLong(1)))).distinct.collect.sortBy(c => c).sliding(2).map(lst => Interval(lst(0), lst(1))).toArray

    val graphs = intervals.map( p =>
      Graph(verts.filter("NOT (estart >= " + p.getEndSeconds + " OR eend <= " + p.getStartSeconds + ")").rdd.map(r => (r.getLong(0), r.getAs[V](3))),
        edgs.filter("NOT (estart >= " + p.getEndSeconds + " OR eend <= " + p.getStartSeconds + ")").rdd.map(r => Edge(r.getLong(0), r.getLong(1), (r.getLong(4),r.getAs[E](5)))),
        defVal, storLevel, storLevel)
    ).par

    new SnapshotGraphParallel(intervals, graphs, defVal, storLevel, coalesced)
  }

  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V):SnapshotGraphParallel[V, E] = new SnapshotGraphParallel(Array[Interval](), ParSeq[Graph[V,(EdgeId,E)]](), defVal, coal = true)
 
}
