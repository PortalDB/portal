package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.parallel.ParSeq
import scala.collection.immutable.BitSet
import scala.collection.mutable.LinkedHashMap
import scala.collection.breakOut

import scala.reflect.ClassTag
import scala.util.control._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.HashPartitioner

import org.apache.spark.graphx._
import org.apache.spark.rdd._

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.MultifileLoad

import java.time.LocalDate

class HybridGraph[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], runs: Seq[Int], gps: ParSeq[Graph[BitSet, BitSet]], veratts: RDD[(VertexId,(TimeIndex,VD))], edgatts: RDD[((VertexId,VertexId),(TimeIndex,ED))]) extends TemporalGraph[VD, ED] with Serializable {
  val graphs: ParSeq[Graph[BitSet, BitSet]] = gps
  val resolution: Resolution = if (intvs.size > 0) intvs.head.resolution else Resolution.zero
  //this is how many consecutive intervals are in each aggregated graph
  val widths: Seq[Int] = runs
  
  if (widths.reduce(_ + _) != intvs.size)
    throw new IllegalArgumentException("temporal sequence and runs do not match")

  intvs.foreach { ii =>
    if (!ii.resolution.isEqual(resolution))
      throw new IllegalArgumentException("temporal sequence is not valid, intervals are not all of equal resolution")
  }

  val intervals: Seq[Interval] = intvs
  lazy val span: Interval = if (intervals.size > 0) Interval(intervals.head.start, intervals.last.end) else Interval(LocalDate.now, LocalDate.now)

  //vertex attributes are kept in a separate rdd with an id key
  //so there are multiple entries for the same key potentially
  val vertexattrs: RDD[(VertexId,(TimeIndex,VD))] = veratts

  //edge attributes are kept in a separate rdd with an id,id key
  val edgeattrs: RDD[((VertexId,VertexId),(TimeIndex,ED))] = edgatts

  /** Default constructor is provided to support serialization */
  protected def this() = this(Seq[Interval](), Seq[Int](0), ParSeq[Graph[BitSet,BitSet]](), ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)

  override def size(): Int = intervals.size

  override def materialize() = {
    graphs.foreach { x =>
      if (!x.edges.isEmpty)
        x.numEdges
      if (!x.vertices.isEmpty)
        x.numVertices
    }
    vertexattrs.count
    edgeattrs.count
  }

  override def vertices: RDD[(VertexId,Map[Interval, VD])] = {
    val start = span.start
    val res = resolution

    vertexattrs.map{ case (k, v) => (k, Map[Interval,VD](res.getInterval(start, v._1) -> v._2))}.reduceByKey{case (a,b) => a ++ b}
  }

  override def verticesFlat: RDD[(VertexId, (Interval, VD))] = {
    val start = span.start
    val res = resolution

    vertexattrs.map{ case (k,v) => (k, (res.getInterval(start, v._1), v._2))}
  }

  override def edges: RDD[((VertexId,VertexId),Map[Interval, ED])] = {
    val start = span.start
    val res = resolution

    edgeattrs.map{ case (k,v) => (k, Map[Interval,ED](res.getInterval(start, v._1) -> v._2))}
      .reduceByKey((a: Map[Interval, ED], b: Map[Interval, ED]) => a ++ b)
  }

  override def edgesFlat: RDD[((VertexId,VertexId),(Interval, ED))] = {
    val start = span.start
    val res = resolution

    edgeattrs.map{ case (k,v) => (k, (res.getInterval(start, v._1), v._2))}
  }

  override def degrees: RDD[(VertexId,Map[Interval, Int])] = {
    val start = span.start
    val res = resolution
    val allgs = graphs.map(Degree.run(_))
    allgs.map(g => g.vertices).reduce((x,y) => VertexRDD(x union y))
      .reduceByKey((a: Map[TimeIndex, Int], b: Map[TimeIndex, Int]) => a ++ b).map{ case (vid,vattr) => (vid, vattr.map{ case (k,v) => (res.getInterval(start, k),v)})}
  }

  override def getTemporalSequence: Seq[Interval] = intervals

  override def getSnapshot(period: Interval): Graph[VD, ED] = {
    val index = intervals.indexOf(period)
    if (index >= 0) {
      //the index of the aggregated graph is based on the width
      val filteredvas: RDD[(VertexId, VD)] = vertexattrs.filter{ case (k,v) => v._1 == index}.map{ case (k,v) => (k, v._2)}
      val filterededs: RDD[Edge[ED]] = edgeattrs.filter{ case (k,v) => v._1 == index}.map{ case (k,v) => Edge(k._1, k._2, v._2)}
      Graph[VD,ED](filteredvas, EdgeRDD.fromEdges[ED,VD](filterededs),
        edgeStorageLevel = getStorageLevel,
        vertexStorageLevel = getStorageLevel)
    } else
      Graph[VD,ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
  }

  /** Query operations */
  
  override def select(bound: Interval): TemporalGraph[VD, ED] = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this

    if (span.intersects(bound)) {
      val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
      val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end
      val start = span.start

      //compute indices of start and stop
      //start is inclusive, stop exclusive
      val selectStart:Int = intervals.indexOf(resolution.getInterval(startBound))
      var selectStop:Int = intervals.indexOf(resolution.getInterval(endBound))
      if (selectStop < 0) selectStop = intervals.size
      val newIntvs: Seq[Interval] = intervals.slice(selectStart, selectStop)

      //compute indices of the aggregates that should be included
      //both inclusive
      val partialSum: Seq[Int] = widths.scanLeft(0)(_ + _).tail
      val indexStart: Int = partialSum.indexWhere(_ >= (selectStart+1))
      val indexStop: Int = partialSum.indexWhere(_ >= selectStop)

      //TODO: rewrite simpler
      val tail: Seq[Int] = if (indexStop == indexStart) Seq() else Seq(widths(indexStop) - partialSum(indexStop) + selectStop)
      val runs: Seq[Int] = if (indexStop == indexStart) Seq(selectStop - selectStart) else Seq(partialSum(indexStart) - selectStart) ++ widths.slice(indexStart, indexStop).drop(1) ++ tail
      //indices to keep in partial graphs
      val stop1: Int = partialSum(indexStart) - 1
      val stop2: Int = partialSum.lift(indexStop-1).getOrElse(0)

      //filter out aggregates where we need only partials
      //drop those we don't need
      //keep the rest
      val subg:ParSeq[Graph[BitSet,BitSet]] = graphs.zipWithIndex.flatMap{ case (g,index) =>
        if (index == indexStart || index == indexStop) {
          val mask: BitSet = if (index == indexStart) BitSet((selectStart to stop1): _*) else BitSet((stop2 to (selectStop-1)): _*)
          Some(g.subgraph(
            vpred = (vid, attr) => !(attr & mask).isEmpty,
            epred = et => !(et.attr & mask).isEmpty)
            .mapVertices((vid, vattr) => vattr.filter(x => x >= selectStart && x < selectStop).map(_ - selectStart)).mapEdges(e => e.attr.filter(x => x > selectStart && x < selectStop).map(_ - selectStart)))
        } else if (index > indexStart && index < indexStop) {
          Some(g.mapVertices((vid, vattr) => vattr.map(_ - selectStart))
            .mapEdges(e => e.attr.map(_ - selectStart)))
        } else
          None
      }

      //now need to update the vertex attribute rdd and edge attr rdd
      val vattrs = vertexattrs.filter{ case (k,v) => v._1 >= selectStart && v._1 < selectStop}.map{ case (k,v) => (k, (v._1 - selectStart, v._2))}
      val eattrs = edgeattrs.filter{ case (k,v) => v._1 >= selectStart && v._1 < selectStop}.map{ case (k,v) => (k, (v._1 - selectStart, v._2))}

      new HybridGraph[VD, ED](newIntvs, runs, subg, vattrs, eattrs)

    } else
      HybridGraph.emptyGraph[VD,ED]()
  }

  override def select(tpred: Interval => Boolean): TemporalGraph[VD, ED] = {
    val start = span.start
    val res = resolution

    val subg:ParSeq[Graph[BitSet,BitSet]] = graphs.map{ g =>
      g.mapVertices((vid, vattr) => vattr.filter(x => tpred(res.getInterval(start, x))))
      .mapEdges(e => e.attr.filter(x => tpred(res.getInterval(start, x))))
      .subgraph(vpred = (vid, attr) => !attr.isEmpty,
        epred = et => !et.attr.isEmpty)
    }

    new HybridGraph[VD, ED](intervals, widths, subg, 
      vertexattrs.filter{ case (k,v) => tpred(res.getInterval(start, v._1))},
      edgeattrs.filter{ case (k,v) => tpred(res.getInterval(start, v._1))})
  }

  override def select(epred: EdgeTriplet[VD, ED] => Boolean, vpred: (VertexId, VD) => Boolean): TemporalGraph[VD, ED] = {
    //two options to do this: join with the values in a map and subgraph
    //or generate triplets from attribute rdds, then join with it
    throw new UnsupportedOperationException("select not yet impelemented")
  }

  override def aggregate(res: Resolution, vsem: AggregateSemantics.Value, esem: AggregateSemantics.Value, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    if (!resolution.isCompatible(res)) {
      throw new IllegalArgumentException("incompatible resolution")
    }

    var intvs: Seq[Interval] = scala.collection.immutable.Seq[Interval]()
    //it is possible that different number of graphs end up in different intervals
    //such as going from days to months
    var index:Int = 0
    val indMap:scala.collection.mutable.ListBuffer[TimeIndex] = scala.collection.mutable.ListBuffer[TimeIndex]()
    //make a map of old indices to new ones
    var counts:scala.collection.immutable.Seq[Int] = scala.collection.immutable.Seq[Int]()

    while (index < intervals.size) {
      val intv:Interval = intervals(index)
      //need to compute the interval start and end based on resolution new units
      val newIntv:Interval = res.getInterval(intv.start)
      val expected:Int = resolution.getNumParts(res, intv.start)

      indMap.insert(index, intvs.size)
      index += 1

      //grab all the intervals that fit within
      val loop = new Breaks
      loop.breakable {
        while (index < intervals.size) {
          val intv2:Interval = intervals(index)
          if (newIntv.contains(intv2)) {
            indMap.insert(index, intvs.size)
            index += 1
          } else {
            loop.break
          }
        }
      }

      counts = counts :+ expected
      intvs = intvs :+ newIntv
    }

    //need to combine graphs such that new intervals don't span graph boundaries
    //at worst this will lead to a single "OneGraph"
    var xx:Int = 0
    var yy:Int = 0
    val countsSum: Seq[Int] = counts.scanLeft(0)(_ + _).tail
    val runsSum: Seq[Int] = widths.scanLeft(0)(_ + _).tail
    var runs: Seq[Int] = Seq[Int]()

    var gps: ParSeq[Graph[BitSet,BitSet]] = ParSeq[Graph[BitSet,BitSet]]()
    //there is no union of two graphs in graphx
    var firstVRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
    var firstERDD: RDD[Edge[BitSet]] = ProgramContext.sc.emptyRDD
    var startx:Int = 0
    var numagg:Int = 0

    while (xx < countsSum.size && yy < runsSum.size) {
      if (yy == (runsSum.size - 1) || countsSum(xx) == runsSum(yy)) {
        firstVRDD = firstVRDD.union(graphs(yy).vertices)
        firstERDD = firstERDD.union(graphs(yy).edges)
        numagg = numagg + 1

        //we to go (p)-1 because counts are 1-based but indices are 0-based
        val parts:Seq[(Int,Int,Int)] = (startx to xx).map(p => (countsSum.lift(p-1).getOrElse(0), countsSum(p)-1, p))
        if (numagg > 1) {
          firstVRDD = firstVRDD.reduceByKey(_ ++ _)
          firstERDD = firstERDD.map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey(_ ++ _).map(x => Edge(x._1._1, x._1._2, x._2))
        }
        gps = gps :+ Graph(VertexRDD(firstVRDD.mapValues{ attr =>
          BitSet() ++ parts.flatMap { case (start, end, index) =>
            val mask = BitSet((start to end): _*)
            if (vsem == AggregateSemantics.All) {
              if (mask.subsetOf(attr))
                Some(index)
              else
                None
            } else if (vsem == AggregateSemantics.Any) {
              if (!(mask & attr).isEmpty)
                Some(index)
              else
                None
            } else None
          }
        }.filter{ case (vid, attr) => !attr.isEmpty}), EdgeRDD.fromEdges[BitSet,BitSet](firstERDD).mapValues{e => 
          BitSet() ++ parts.flatMap { case (start, end, index) =>
            val mask = BitSet((start to end): _*)
            if (esem == AggregateSemantics.All) {
              if (mask.subsetOf(e.attr))
                Some(index)
              else
                None
            } else if (esem == AggregateSemantics.Any) {
              if (!(mask & e.attr).isEmpty)
                Some(index)
              else
                None
            } else None
          }
        }.filter{ e => !e.attr.isEmpty},
          BitSet(), edgeStorageLevel = getStorageLevel,
          vertexStorageLevel = getStorageLevel)
        //the number of snapshots in this new aggregate
        runs = runs :+ (countsSum(xx) - countsSum.lift(startx-1).getOrElse(0))

        //reset, move on
        firstVRDD = ProgramContext.sc.emptyRDD
        firstERDD = ProgramContext.sc.emptyRDD
        xx = xx+1
        yy = yy+1
        startx = xx
        numagg = 0
      } else if (countsSum(xx) < runsSum(yy)) {
        xx = xx+1
      } else { //runsSum(y) < countsSum(x)
        firstVRDD = firstVRDD.union(graphs(yy).vertices)
        firstERDD = firstERDD.union(graphs(yy).edges)
        yy = yy+1
        numagg = numagg + 1
      }
    }

    //TODO: do this more efficiently
    val broadcastIndMap = ProgramContext.sc.broadcast(indMap.toSeq)
    //TODO: make this hard-coded parameter be dependent on evolution rate
    val aveReductFactor = math.max(1, counts.head / 2)
    val vattrs = if (vsem == AggregateSemantics.All) vertexattrs.map{ case (k,v) => ((k, broadcastIndMap.value(v._1)), (v._2, 1))}.reduceByKey((x,y) => (vAggFunc(x._1, y._1), x._2 + y._2), math.max(4, vertexattrs.partitions.size / aveReductFactor)).filter{ case (k, (attr,cnt)) => cnt == counts(k._2)}.map{ case (k,v) => (k._1, (k._2, v._1))} else vertexattrs.map{ case (k,v) => ((k, broadcastIndMap.value(v._1)), v._2)}.reduceByKey(vAggFunc, math.max(4, vertexattrs.partitions.size / aveReductFactor)).map{ case (k,v) => (k._1, (k._2, v))}
    //FIXME: this does not filter out edges for which vertices no longer exist
    //such as would happen when vertex semantics is universal
    //and edges semantics existential
    val eattrs = if (esem == AggregateSemantics.All) 
      edgeattrs.map{ case (k,v) => ((k._1, k._2, broadcastIndMap.value(v._1)), (v._2, 1))}.reduceByKey((x,y) => (eAggFunc(x._1, y._1), x._2 + y._2), math.max(4, edgeattrs.partitions.size / aveReductFactor)).filter{ case (k, (attr,cnt)) => cnt == counts(k._3)}.map{ case (k,v) => ((k._1, k._2), (k._3, v._1))}
    else 
      edgeattrs.map{ case (k,v) => ((k._1, k._2, broadcastIndMap.value(v._1)), v._2)}.reduceByKey(eAggFunc, math.max(4, edgeattrs.partitions.size / aveReductFactor)).map{ case (k,v) => ((k._1, k._2), (k._3, v))}

    new HybridGraph[VD, ED](intvs, runs, gps, vattrs, eattrs)
  }

  override def transform[ED2: ClassTag, VD2: ClassTag](emap: (Edge[ED], Interval) => ED2, vmap: (VertexId, Interval, VD) => VD2): TemporalGraph[VD2, ED2] = {
    //we assign these to local vals for closure reasons
    val start = span.start
    val res = resolution

    new HybridGraph[VD2, ED2](intervals, widths, graphs, vertexattrs.map{ case (k,v) => (k, (v._1, vmap(k, res.getInterval(start, v._1), v._2)))}, edgeattrs.map{ case (k,v) => (k, (v._1, emap(Edge(k._1, k._2, v._2), res.getInterval(start, v._1))))})
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val start = span.start
    val res = resolution
    new HybridGraph[VD2, ED](intervals, widths, graphs, vertexattrs.map{ case (k,v) => (k, (v._1, map(k, res.getInterval(start, v._1), v._2)))}, edgeattrs)
  }

  override def mapVerticesWIndex[VD2: ClassTag](map: (VertexId, TimeIndex, VD) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    new HybridGraph[VD2, ED](intervals, widths, graphs, vertexattrs.map{ case (k,v) => (k, (v._1, map(k, v._1, v._2)))}, edgeattrs)
  }

  override def mapEdges[ED2: ClassTag](map: (Edge[ED], Interval) => ED2): TemporalGraph[VD, ED2] = {
    val start = span.start
    val res = resolution
    new HybridGraph[VD, ED2](intervals, widths, graphs, vertexattrs, edgeattrs.map{ case (k,v) => (k, (v._1, map(Edge(k._1, k._2, v._2), res.getInterval(start, v._1))))})
  }

  override def mapEdgesWIndex[ED2: ClassTag](map: (Edge[ED], TimeIndex) => ED2): TemporalGraph[VD, ED2] = {
    new HybridGraph[VD, ED2](intervals, widths, graphs, vertexattrs, edgeattrs.map{ case (k,v) => (k, (v._1, map(Edge(k._1, k._2, v._2), v._1)))})
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, Map[Interval, U])])(mapFunc: (VertexId, Interval, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val start = span.start
    val res = resolution

    val in: RDD[((VertexId,TimeIndex),U)] = other.flatMap(x => x._2.map(y => ((x._1, res.numBetween(start, y._1.start)), y._2)))

    new HybridGraph[VD2, ED](intervals, widths, graphs, vertexattrs.map{ case (k,v) => ((k, v._1), v._2)}.leftOuterJoin(in).map{ case (k,v) => (k._1, (k._2, mapFunc(k._1, res.getInterval(start, k._2), v._1, v._2)))}, edgeattrs)
  }

  override def union(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    var grp2: HybridGraph[VD, ED] = other match {
      case grph: HybridGraph[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (!intervals.head.isUnionCompatible(grp2.intervals.head)) {
      throw new IllegalArgumentException("two graphs are not union-compatible in the temporal schema")
    }

    //compute the combined span
    val startBound = if (span.start.isBefore(grp2.span.start)) span.start else grp2.span.start
    val endBound = if (span.end.isAfter(grp2.span.end)) span.end else grp2.span.end

    //compute the new intervals
    //because the temporal sequence is consecutive and of the same resolution
    //it is easy to generate from the span
    var mergedIntervals: Seq[Interval] = Seq[Interval]()
    var xx:LocalDate = startBound
    while (xx.isBefore(endBound)) {
      val nextInterval = resolution.getInterval(xx)
      mergedIntervals = mergedIntervals :+ nextInterval
      xx = nextInterval.end
    }

    //pad the sequences with single snapshots to retain graphs where no intersection
    val gr1IndexStart:Int = resolution.numBetween(startBound, span.start).toInt
    val gr2IndexStart:Int = resolution.numBetween(startBound, grp2.span.start).toInt
    val gr1Pad:Long = resolution.numBetween(span.end, endBound)
    val gr2Pad:Long = resolution.numBetween(grp2.span.end, endBound)
    val grseq1start:ParSeq[Graph[BitSet, BitSet]] = ParSeq.fill(gr1IndexStart){Graph[BitSet, BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)}
    val grseq1pad:ParSeq[Graph[BitSet, BitSet]] = ParSeq.fill(gr1Pad.toInt){Graph[BitSet, BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)}
    val grseq1:ParSeq[Graph[BitSet, BitSet]] =  grseq1start ++ graphs ++ grseq1pad
    val grseq2start:ParSeq[Graph[BitSet, BitSet]] = ParSeq.fill(gr2IndexStart){Graph[BitSet, BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)}
    val grseq2pad:ParSeq[Graph[BitSet, BitSet]] = ParSeq.fill(gr2Pad.toInt){Graph[BitSet, BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)}
    val grseq2:ParSeq[Graph[BitSet, BitSet]] = grseq2start ++ grp2.graphs ++ grseq2pad

    val gr1Sums: Seq[Int] = (Seq.fill(gr1IndexStart){1} ++ widths ++ Seq.fill(gr1Pad.toInt){1}).scanLeft(0)(_ + _).tail
    val gr2Sums: Seq[Int] = (Seq.fill(gr2IndexStart){1} ++ grp2.widths ++ Seq.fill(gr2Pad.toInt){1}).scanLeft(0)(_ + _).tail

    //have to align the subgraphs
    //like in aggregation
    var gr1Index:Int = 0
    var gr2Index:Int = 0
    var runs: Seq[Int] = Seq[Int]()
    var gps: ParSeq[Graph[BitSet,BitSet]] = ParSeq[Graph[BitSet,BitSet]]()
    //there is no union of two graphs in graphx
    var gr1VRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
    var gr1ERDD: RDD[Edge[BitSet]] = ProgramContext.sc.emptyRDD
    var gr2VRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
    var gr2ERDD: RDD[Edge[BitSet]] = ProgramContext.sc.emptyRDD
    var num1agg:Int = 0
    var num2agg:Int = 0
    //how many we have accumulated so far
    var snapshotCount:Int = 0

    while (gr1Index < gr1Sums.size && gr2Index < gr2Sums.size) {
      //if aligned, union and apply functions/semantics
      if (gr1Sums(gr1Index) == gr2Sums(gr2Index)) {
        gr1VRDD = gr1VRDD.union(grseq1(gr1Index).vertices)
        gr2VRDD = gr2VRDD.union(grseq2(gr2Index).vertices)
        gr1ERDD = gr1ERDD.union(grseq1(gr1Index).edges)
        gr2ERDD = gr2ERDD.union(grseq2(gr2Index).edges)
        num1agg = num1agg + 1
        num2agg = num2agg + 1

        if (num1agg > 1) {
          gr1VRDD = gr1VRDD.reduceByKey(_ ++ _)
          gr1ERDD = gr1ERDD.map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey(_ ++ _).map(x => Edge(x._1._1, x._1._2, x._2))
        }
        if (num2agg > 1) {
          gr2VRDD = gr2VRDD.reduceByKey(_ ++ _)
          gr2ERDD = gr2ERDD.map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey(_ ++ _).map(x => Edge(x._1._1, x._1._2, x._2))
        }

        if (gr1IndexStart > 0) {
          gr1VRDD = gr1VRDD.mapValues{ vattr:BitSet => vattr.map(x => x + gr1IndexStart)}
          gr1ERDD = gr1ERDD.map{ e => Edge(e.srcId, e.dstId, e.attr.map(_ + gr1IndexStart))}
        }
        if (gr2IndexStart > 0) {
          gr2VRDD = gr2VRDD.mapValues{ vattr:BitSet => vattr.map(_ + gr2IndexStart)}
          gr2ERDD = gr2ERDD.map{ e => Edge(e.srcId, e.dstId, e.attr.map(_ + gr2IndexStart))}
        }

        val newverts = if (sem == AggregateSemantics.All) (gr1VRDD union gr2VRDD).reduceByKey{ (a: BitSet, b: BitSet) => a & b }.filter{ case (vid, vattr) => !vattr.isEmpty} else (gr1VRDD union gr2VRDD).reduceByKey{ (a: BitSet, b: BitSet) => a.union(b) }
        val newedges = if (sem == AggregateSemantics.All) (gr1ERDD union gr2ERDD).map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey{ (a: BitSet, b: BitSet) => a & b }.filter{ x => !x._2.isEmpty}.map{ case (k,v) => Edge(k._1, k._2, v)} else (gr1ERDD union gr2ERDD).map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey{ (a: BitSet, b: BitSet) => a.union(b) }.map{ case (k,v) => Edge(k._1, k._2, v)}

        gps = gps :+ Graph(VertexRDD(newverts), EdgeRDD.fromEdges[BitSet,BitSet](newedges), BitSet(), edgeStorageLevel = getStorageLevel, vertexStorageLevel = getStorageLevel)
        //how many snapshots in this graph
        runs = runs :+ (gr1Sums(gr1Index) - snapshotCount)

        //done, now reset and keep going
        snapshotCount = gr1Sums(gr1Index)
        gr1VRDD = ProgramContext.sc.emptyRDD
        gr2VRDD = ProgramContext.sc.emptyRDD
        gr1ERDD = ProgramContext.sc.emptyRDD
        gr2ERDD = ProgramContext.sc.emptyRDD
        gr1Index = gr1Index+1
        gr2Index = gr2Index+1
        num1agg = 0
        num2agg = 0
      } else if (gr1Sums(gr1Index) < gr2Sums(gr2Index)) { //accumulate graph1 groups
        gr1VRDD = gr1VRDD.union(grseq1(gr1Index).vertices)
        gr1ERDD = gr1ERDD.union(grseq1(gr1Index).edges)
        gr1Index = gr1Index+1
        num1agg = num1agg + 1
      } else { //gr2Sums(gr2Index) < gr1Sums(gr1Index)
        gr2VRDD = gr2VRDD.union(grseq2(gr2Index).vertices)
        gr2ERDD = gr2ERDD.union(grseq2(gr2Index).edges)
        gr2Index = gr2Index+1
        num2agg = num2agg + 1
      }
    }

    //now the values
    val gr1vattrs = vertexattrs.map{ case (k,v) => ((k, v._1 + gr1IndexStart), v._2)}
    val gr2vattrs = grp2.vertexattrs.map{ case (k,v) => ((k, v._1 + gr2IndexStart), v._2)}
    //now put them together
    val vattrs = if (sem == AggregateSemantics.All) (gr1vattrs join gr2vattrs).map{ case (k,v) => (k._1, (k._2, vFunc(v._1, v._2)))} else (gr1vattrs union gr2vattrs).reduceByKey(vFunc).map{ case (k,v) => (k._1, (k._2, v))}
    val gr1eattrs = edgeattrs.map{ case (k,v) => ((k._1, k._2, v._1 + gr1IndexStart), v._2)}
    val gr2eattrs = grp2.edgeattrs.map{ case (k,v) => ((k._1, k._2, v._1 + gr2IndexStart), v._2)}
    val eattrs = if (sem == AggregateSemantics.All) (gr1eattrs join gr2eattrs).map{ case (k,v) => ((k._1, k._2), (k._3, eFunc(v._1, v._2)))} else (gr1eattrs union gr2eattrs).reduceByKey(eFunc).map{ case (k,v) => ((k._1, k._2), (k._3, v))}

    new HybridGraph[VD, ED](mergedIntervals, runs, gps, vattrs, eattrs)
  }

  override def intersect(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    /** The type checking already validates that the structurally the graphs are union-compatible
      * But we also need to check that they are temporally union-compatible
      * this includes having the same resolution and aligning intervals
      * Two temporal sequences are compatible if their first elements are compatible
      */ 
    var grp2: HybridGraph[VD, ED] = other match {
      case grph: HybridGraph[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (!intervals.head.isUnionCompatible(grp2.intervals.head)) {
      throw new IllegalArgumentException("two graphs are not union-compatible in the temporal schema")
    }

    //compute the combined span
    val startBound = if (span.start.isBefore(grp2.span.start)) grp2.span.start else span.start
    val endBound = if (span.end.isAfter(grp2.span.end)) grp2.span.end else span.end
    if (startBound.isAfter(endBound) || startBound.isEqual(endBound)) {
      HybridGraph.emptyGraph[VD,ED]()
    } else {
      //we are taking a temporal subset of both graphs
      //and then doing the structural part
      val gr1Sel = select(Interval(startBound, endBound))  match {
        case grph: HybridGraph[VD, ED] => grph
        case _ => throw new ClassCastException
      }
      val gr2Sel = grp2.select(Interval(startBound, endBound)) match {
        case grph: HybridGraph[VD, ED] => grph
        case _ => throw new ClassCastException
      }

      //have to align the subgraphs
      //like in aggregation
      val gr1Sums: Seq[Int] = gr1Sel.widths.scanLeft(0)(_ + _).tail
      val gr2Sums: Seq[Int] = gr2Sel.widths.scanLeft(0)(_ + _).tail
      var gr1Index:Int = 0
      var gr2Index:Int = 0
      var runs: Seq[Int] = Seq[Int]()
      var gps: ParSeq[Graph[BitSet,BitSet]] = ParSeq[Graph[BitSet,BitSet]]()
      //there is no union of two graphs in graphx
      var gr1VRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
      var gr1ERDD: RDD[Edge[BitSet]] = ProgramContext.sc.emptyRDD
      var gr2VRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
      var gr2ERDD: RDD[Edge[BitSet]] = ProgramContext.sc.emptyRDD
      var num1agg:Int = 0
      var num2agg:Int = 0
      //how many we have accumulated so far
      var snapshotCount:Int = 0

      while (gr1Index < gr1Sums.size && gr2Index < gr2Sums.size) {
        //if aligned, union and apply functions/semantics
        if (gr1Sums(gr1Index) == gr2Sums(gr2Index)) {
          gr1VRDD = gr1VRDD.union(gr1Sel.graphs(gr1Index).vertices)
          gr2VRDD = gr2VRDD.union(gr2Sel.graphs(gr2Index).vertices)
          gr1ERDD = gr1ERDD.union(gr1Sel.graphs(gr1Index).edges)
          gr2ERDD = gr2ERDD.union(gr2Sel.graphs(gr2Index).edges)
          num1agg = num1agg + 1
          num2agg = num2agg + 1

          if (num1agg > 1) {
            gr1VRDD = gr1VRDD.reduceByKey(_ ++ _)
            gr1ERDD = gr1ERDD.map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey(_ ++ _).map(x => Edge(x._1._1, x._1._2, x._2))
          }
          if (num2agg > 1) {
            gr2VRDD = gr2VRDD.reduceByKey(_ ++ _)
            gr2ERDD = gr2ERDD.map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey(_ ++ _).map(x => Edge(x._1._1, x._1._2, x._2))
          }

          val newverts = if (sem == AggregateSemantics.All) (gr1VRDD union gr2VRDD).reduceByKey{ (a: BitSet, b: BitSet) => a & b }.filter{ case (vid, vattr) => !vattr.isEmpty} else (gr1VRDD union gr2VRDD).reduceByKey{ (a: BitSet, b: BitSet) => a.union(b) }
          val newedges = if (sem == AggregateSemantics.All) (gr1ERDD union gr2ERDD).map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey{ (a: BitSet, b: BitSet) => a & b }.filter{ x => !x._2.isEmpty}.map{ case (k,v) => Edge(k._1, k._2, v)} else (gr1ERDD union gr2ERDD).map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey{ (a: BitSet, b: BitSet) => a.union(b) }.map{ case (k,v) => Edge(k._1, k._2, v)}

          gps = gps :+ Graph(VertexRDD(newverts), EdgeRDD.fromEdges[BitSet,BitSet](newedges), BitSet(), edgeStorageLevel = getStorageLevel, vertexStorageLevel = getStorageLevel)
          //how many snapshots in this graph
          runs = runs :+ (gr1Sums(gr1Index) - snapshotCount)

          //done, now reset and keep going
          snapshotCount = gr1Sums(gr1Index)
          gr1VRDD = ProgramContext.sc.emptyRDD
          gr2VRDD = ProgramContext.sc.emptyRDD
          gr1ERDD = ProgramContext.sc.emptyRDD
          gr2ERDD = ProgramContext.sc.emptyRDD
          gr1Index = gr1Index+1
          gr2Index = gr2Index+1
          num1agg = 0
          num2agg = 0
        } else if (gr1Sums(gr1Index) < gr2Sums(gr2Index)) { //accumulate graph1 groups
          gr1VRDD = gr1VRDD.union(graphs(gr1Index).vertices)
          gr1ERDD = gr1ERDD.union(graphs(gr1Index).edges)
          gr1Index = gr1Index+1
          num1agg = num1agg + 1
        } else { //gr2Sums(gr2Index) < gr1Sums(gr1Index)
          gr2VRDD = gr2VRDD.union(grp2.graphs(gr2Index).vertices)
          gr2ERDD = gr2ERDD.union(grp2.graphs(gr2Index).edges)
          gr2Index = gr2Index+1
          num2agg = num2agg + 1
        }
      }

      val vattrs = if (sem == AggregateSemantics.All) (gr1Sel.vertexattrs.map{ case (k,v) => ((k, v._1), v._2)} join gr2Sel.vertexattrs.map{ case (k,v) => ((k, v._1), v._2)}).map{ case (k,v) => (k._1, (k._2, vFunc(v._1, v._2)))} else (gr1Sel.vertexattrs.map{ case (k,v) => ((k, v._1), v._2)} union gr2Sel.vertexattrs.map{ case (k,v) => ((k, v._1), v._2)}).reduceByKey(vFunc).map{ case (k,v) => (k._1, (k._2, v))}
      val eattrs = if (sem == AggregateSemantics.All) (gr1Sel.edgeattrs.map{ case (k,v) => ((k._1, k._2, v._1), v._2)} join gr2Sel.edgeattrs.map{ case (k,v) => ((k._1, k._2, v._1), v._2)}).map{ case (k,v) => ((k._1, k._2), (k._3, eFunc(v._1, v._2)))} else (gr1Sel.edgeattrs.map{ case (k,v) => ((k._1, k._2, v._1), v._2)} union gr2Sel.edgeattrs.map{ case (k,v) => ((k._1, k._2, v._1), v._2)}).reduceByKey(eFunc).map{case (k,v) => ((k._1, k._2), (k._3, v))}

      new HybridGraph[VD, ED](gr1Sel.intervals, runs, gps, vattrs, eattrs)
    }
  }

  override def pregel[A: ClassTag]
  (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
    activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
    sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A): TemporalGraph[VD, ED] = {
    throw new UnsupportedOperationException("pregel not yet implemented")
  }

  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): TemporalGraph[Double, Double] = {
    val runSums = widths.scanLeft(0)(_ + _).tail

    if (uni) {
      def prank(grp: Graph[BitSet,BitSet], minIndex: Int, maxIndex: Int): Graph[LinkedHashMap[TimeIndex,(Double,Double)], LinkedHashMap[TimeIndex,(Double,Double)]] = {
        if (grp.edges.isEmpty)
          Graph[LinkedHashMap[TimeIndex,(Double,Double)],LinkedHashMap[TimeIndex,(Double,Double)]](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
        else {
          UndirectedPageRank.runHybrid(grp, minIndex, maxIndex-1, tol, resetProb, numIter)
        }
      }
    
      val allgs:ParSeq[Graph[LinkedHashMap[TimeIndex,(Double,Double)], LinkedHashMap[TimeIndex,(Double,Double)]]] = graphs.zipWithIndex.map{ case (g,i) => prank(g, runSums.lift(i-1).getOrElse(0), runSums(i))}

      //now extract values
      val vattrs= allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.toSeq.map{ case (k,v) => (vid, (k, v._1))}}}.reduce(_ union _)
      val eattrs = allgs.map{ g => g.edges.flatMap{ e => e.attr.toSeq.map{ case (k,v) => ((e.srcId, e.dstId), (k, v._1))}}}.reduce(_ union _)

      new HybridGraph(intervals, widths, graphs, vattrs, eattrs)

    } else
      throw new UnsupportedOperationException("directed version of pagerank not yet implemented")
  }

/*
  //FIXME: this should be [Int,ED]
  override def degree(): TemporalGraph[Double, Double] = {
    val allgs = graphs.map(Degree.run(_))

    //now extract values
    val vattrs = allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.toSeq.map{ case (k,v) => (vid, (k, v.toDouble))}}}.reduce(_ union _)
    val eattrs = edgeattrs.map{ case (k, attr) => (k, (attr._1, 0.0))}

    new HybridGraph(intervals, widths, graphs, vattrs, eattrs)
  }
 */

  override def connectedComponents(): TemporalGraph[VertexId, ED] = {
    val runSums = widths.scanLeft(0)(_ + _).tail

    def conc(grp: Graph[BitSet,BitSet], minIndex: Int, maxIndex: Int): Graph[LinkedHashMap[TimeIndex,VertexId],BitSet] = {
    if (grp.vertices.isEmpty)
        Graph[LinkedHashMap[TimeIndex,VertexId],BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      else {
        ConnectedComponentsXT.runHybrid(grp, minIndex, maxIndex-1)
      }
    }

    val allgs = graphs.zipWithIndex.map{ case (g,i) => conc(g, runSums.lift(i-1).getOrElse(0), runSums(i))}

    //now extract values
    val vattrs = allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.toSeq.map{ case (k,v) => (vid, (k, v))}}}.reduce(_ union _)

    new HybridGraph(intervals, widths, graphs, vattrs, edgeattrs)
  }

  override def shortestPaths(landmarks: Seq[VertexId]): TemporalGraph[ShortestPathsXT.SPMap, ED] = {
    throw new UnsupportedOperationException("shortest paths not yet implemented")
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    graphs.filterNot(_.edges.isEmpty).map(_.edges.partitions.size).reduce(_ + _)
  }

  def getStorageLevel: StorageLevel = graphs.head.edges.getStorageLevel

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): TemporalGraph[VD, ED] = {
    //persist each graph if it is not yet persisted
    graphs.map(g => g.persist(newLevel))
    vertexattrs.persist(newLevel)
    edgeattrs.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): TemporalGraph[VD, ED] = {
    graphs.map(_.unpersist(blocking))
    vertexattrs.unpersist(blocking)
    edgeattrs.unpersist(blocking)
    this
  }
  
  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int): TemporalGraph[VD, ED] = {
    partitionBy(pst, runs, 0)
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): TemporalGraph[VD, ED] = {
    if (pst != PartitionStrategyType.None) {
      new HybridGraph(intervals, widths, graphs.zipWithIndex.map { case (g,index) =>
        val numParts: Int = if (parts > 0) parts else g.edges.partitions.size
        g.partitionBy(PartitionStrategies.makeStrategy(pst, index, intervals.size, runs), numParts)}, vertexattrs, edgeattrs)
    } else
      this
  }

}

object HybridGraph extends Serializable {
  final def loadData(dataPath: String, start: LocalDate, end: LocalDate): HybridGraph[String, Int] = {
    loadWithPartition(dataPath, start, end, PartitionStrategyType.None, 1)
  }

  final def loadWithPartition(dataPath: String, start: LocalDate, end: LocalDate, strategy: PartitionStrategyType.Value, runWidth: Int): HybridGraph[String, Int] = {
    var minDate: LocalDate = start
    var maxDate: LocalDate = end

    var source: scala.io.Source = null
    var fs: FileSystem = null

    val pt: Path = new Path(dataPath + "/Span.txt")
    val conf: Configuration = new Configuration()    
    if (System.getenv("HADOOP_CONF_DIR") != "") {
      conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"))
    }
    fs = FileSystem.get(conf)
    source = scala.io.Source.fromInputStream(fs.open(pt))

    val lines = source.getLines
    val minin = LocalDate.parse(lines.next)
    val maxin = LocalDate.parse(lines.next)
    val res = Resolution.from(lines.next)

    if (minin.isAfter(start)) 
      minDate = minin
    if (maxin.isBefore(end)) 
      maxDate = maxin
    source.close()

    if (minDate.isAfter(maxDate) || minDate.isEqual(maxDate))
      throw new IllegalArgumentException("invalid date range")

    var intvs: Seq[Interval] = scala.collection.immutable.Seq[Interval]()
    var gps: ParSeq[Graph[BitSet,BitSet]] = ParSeq[Graph[BitSet,BitSet]]()
    var vatts: RDD[(VertexId,(TimeIndex,String))] = ProgramContext.sc.emptyRDD
    var eatts: RDD[((VertexId,VertexId),(TimeIndex,Int))] = ProgramContext.sc.emptyRDD
    var xx: LocalDate = minDate
    while (xx.isBefore(maxDate)) {
      intvs = intvs :+ res.getInterval(xx)
      xx = intvs.last.end
    }
    var runs: Seq[Int] = Seq[Int]()

    xx = minDate
    while (xx.isBefore(maxDate)) {
      //TODO: make this more flexible based on similarity measure
      val remaining = res.numBetween(xx, maxDate) - 1
      val take = math.min(runWidth-1, remaining)
      var end = res.getInterval(xx, take)
      runs = runs :+ (take+1)

      //load some number of consecutive graphs into one
      val usersnp: RDD[(VertexId,(TimeIndex,String))] = MultifileLoad.readNodes(dataPath, xx, end.start).flatMap{ x =>
        val (filename, line) = x
        val dt = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))
        val parts = line.split(",")
        val index = res.numBetween(minDate, dt)
        if (parts.size > 1 && parts.head != "" && index > -1)
          Some((parts.head.toLong, (index, parts(1).toString)))
        else
          None
      }
      val users = usersnp.partitionBy(new HashPartitioner(usersnp.partitions.size)).persist
      val linksnp: RDD[((VertexId,VertexId),(TimeIndex,Int))] = MultifileLoad.readEdges(dataPath, xx, end.start).flatMap{ x =>
        val (filename, line) = x
        val dt = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          val attr = if(lineArray.length > 2) lineArray(2).toInt else 0
          val index = res.numBetween(minDate, dt)
          if (srcId > dstId)
            Some(((dstId, srcId), (index,attr)))
          else
            Some(((srcId, dstId), (index,attr)))
        } else None
      }
      val links = linksnp.partitionBy(new HashPartitioner(linksnp.partitions.size)).persist
      //TODO: make these hard-coded parameters be dependent on  data size
      //and evolution rate
      val reductFact: Int = math.max(1, (take+1)/2)
      val verts: RDD[(VertexId, BitSet)] = users.mapValues{ v => BitSet(v._1)}.reduceByKey((a,b) => a union b, math.max(4,users.partitions.size/reductFact) )
      val edges = EdgeRDD.fromEdges[BitSet, BitSet](links.mapValues{ v => BitSet(v._1)}.reduceByKey((a,b) => a union b, math.max(4,links.partitions.size/reductFact)).map{case (k,v) => Edge(k._1, k._2, v)})
      //TODO: make the storage level dependent on size
      var graph: Graph[BitSet,BitSet] = Graph(verts, edges, BitSet(), edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)

      if (strategy != PartitionStrategyType.None) {
        graph = graph.partitionBy(PartitionStrategies.makeStrategy(strategy, 0, intvs.size, runWidth))
      }

      gps = gps :+ graph
      vatts = vatts union users
      eatts = eatts union links
      xx = end.end

      /*
       val degs: RDD[Double] = graph.degrees.map{ case (vid,attr) => attr}
       println("min degree: " + degs.min)
       println("max degree: " + degs.max)
       println("average degree: " + degs.mean)
       val counts = degs.histogram(Array(0.0, 10, 50, 100, 1000, 5000, 10000, 50000, 100000, 250000))
       println("histogram:" + counts.mkString(","))
       println("number of vertices: " + graph.vertices.count)
       println("number of edges: " + graph.edges.count)
       println("number of partitions in edges: " + graph.edges.partitions.size)
      */

    }

    new HybridGraph(intvs, runs, gps, vatts, eatts)
  }

  def emptyGraph[VD: ClassTag, ED: ClassTag](): HybridGraph[VD, ED] = new HybridGraph(Seq[Interval](), Seq[Int](0), ParSeq[Graph[BitSet,BitSet]](), ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
}

object Degree {
  def run(graph: Graph[BitSet,BitSet]): Graph[Map[TimeIndex,Int],BitSet] = {
    def mergeFunc(a:LinkedHashMap[TimeIndex,Int], b:LinkedHashMap[TimeIndex,Int]): LinkedHashMap[TimeIndex,Int] = {
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    val degRDD = graph.aggregateMessages[LinkedHashMap[TimeIndex, Int]](
      ctx => {
        ctx.sendToSrc(LinkedHashMap[TimeIndex,Int]() ++ ctx.attr.seq.map(x => (x,1)))
        ctx.sendToDst(LinkedHashMap[TimeIndex,Int]() ++ ctx.attr.seq.map(x => (x,1)))
      },
      mergeFunc, TripletFields.None)
    graph.outerJoinVertices(degRDD) {
      case (vid, vdata, Some(deg)) => (deg ++ vdata.filter(x => !deg.contains(x)).seq.map(x => (x,0))).toMap
      case (vid, vdata, None) => vdata.seq.map(x => (x,0)).toMap
    }
  }
}
