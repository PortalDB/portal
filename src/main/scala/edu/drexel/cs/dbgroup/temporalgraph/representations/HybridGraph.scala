package edu.drexel.cs.dbgroup.temporalgraph.representations

import java.util.Map
import it.unimi.dsi.fastutil.ints.{Int2DoubleOpenHashMap, Int2ObjectOpenHashMap, Int2IntOpenHashMap, Int2LongOpenHashMap}
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap

import scala.collection.JavaConversions._
import scala.collection.parallel.ParSeq
import scala.collection.immutable.BitSet
import scala.collection.mutable.HashMap

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
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps._

import java.time.LocalDate

class HybridGraph[VD: ClassTag, ED: ClassTag](intvs: Array[Interval], verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[TEdge[ED]], runs: Seq[Int], gps: ParSeq[Graph[BitSet, (EdgeId,BitSet)]], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends VEGraph[VD, ED](verts, edgs, defValue, storLevel, coal) with Serializable {

  var graphs: ParSeq[Graph[BitSet, (EdgeId,BitSet)]] = gps
  //this is how many consecutive intervals are in each aggregated graph
  var widths: Seq[Int] = runs
  private val collectedIntervals: Array[Interval] = intvs
  protected var partitioning = TGraphPartitioning(PartitionStrategyType.None, 1, 0)

/*
  {
    val size = intvs.count
    if (widths.size > 0 && size > 0) {
      if (widths.reduce(_ + _) != size)
        throw new IllegalArgumentException("temporal sequence and runs do not match: " + size + ", widthssum: " + widths.reduce(_+_))
    } else if (!(widths.size == 0 && size == 0)) {
      throw new IllegalArgumentException("temporal sequence and runs do not match")
    }
  }
 */

  override def computeSpan: Interval = if (collectedIntervals.size > 0) Interval(collectedIntervals.head.start, collectedIntervals.last.end) else Interval(LocalDate.now, LocalDate.now)

  /** Query operations */
  
  override def slice(bound: Interval): HybridGraph[VD, ED] = {
    if (graphs.size < 1) return super.slice(bound).partitionBy(partitioning).asInstanceOf[HybridGraph[VD,ED]]
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this
    
    if (span.intersects(bound)) {
      if (graphs.size < 1) computeGraphs()

      val startBound = maxDate(span.start, bound.start)
      val endBound = minDate(span.end, bound.end)
      val selectBound:Interval = Interval(startBound, endBound)
      val start = span.start

      //compute indices of start and stop
      //start and stop both inclusive
      val selectStart:Int = collectedIntervals.indexWhere(ii => ii.intersects(selectBound))
      val selectStop:Int = collectedIntervals.lastIndexWhere(ii => ii.intersects(selectBound))

      //compute indices of the aggregates that should be included
      //both inclusive
      val partialSum: Seq[Int] = widths.scanLeft(0)(_ + _).tail
      val indexStart: Int = partialSum.indexWhere(_ >= (selectStart+1))
      val indexStop: Int = partialSum.indexWhere(_ >= selectStop)

      //TODO: rewrite simpler
      val tail: Seq[Int] = if (indexStop == indexStart) Seq() else Seq(widths(indexStop) - partialSum(indexStop) + selectStop + 1)
      val runs: Seq[Int] = if (indexStop == indexStart) Seq(selectStop - selectStart + 1) else Seq(partialSum(indexStart) - selectStart) ++ widths.slice(indexStart, indexStop).drop(1) ++ tail

      //indices to keep in partial graphs
      val stop1: Int = partialSum(indexStart) - 1
      val stop2: Int = partialSum.lift(indexStop-1).getOrElse(0)

      //filter out aggregates where we need only partials
      //drop those we don't need
      //keep the rest
      val subg:ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = graphs.zipWithIndex.flatMap{ case (g,index) =>
        if (index == indexStart || index == indexStop) {
          val mask: BitSet = if (index == indexStart) BitSet((selectStart to stop1): _*) else BitSet((stop2 to selectStop): _*)
          Some(g.subgraph(
            vpred = (vid, attr) => !(attr & mask).isEmpty,
            epred = et => !(et.attr._2 & mask).isEmpty)
            .mapVertices((vid, vattr) => vattr.filter(x => x >= selectStart && x <= selectStop).map(_ - selectStart))
            .mapEdges(e => (e.attr._1, e.attr._2.filter(x => x >= selectStart && x <= selectStop).map(_ - selectStart))))
        } else if (index > indexStart && index < indexStop) {
          Some(g.mapVertices((vid, vattr) => vattr.map(_ - selectStart))
            .mapEdges(e => (e.attr._1, e.attr._2.map(_ - selectStart))))
        } else
          None
      }

      //now need to update the vertex attribute rdd and edge attr rdd
      val vattrs = allVertices.filter{ case (k,v) => v._1.intersects(selectBound)}.mapValues( v => (Interval(maxDate(v._1.start, startBound), minDate(v._1.end, endBound)), v._2))
      val eattrs = allEdges.filter{ te => te.interval.intersects(selectBound)}
        .map{ te =>
        te.interval = Interval(maxDate(te.interval.start, startBound), minDate(te.interval.end, endBound))
        te
      }
      val newIntvs: Array[Interval] = collectedIntervals.slice(selectStart, selectStop+1).map(intv => if (intv.start.isBefore(startBound) || intv.end.isAfter(endBound)) intv.intersection(selectBound).get else intv)

      new HybridGraph[VD, ED](newIntvs, vattrs, eattrs, runs, subg, defaultValue, storageLevel, coalesced)

    } else
      HybridGraph.emptyGraph[VD,ED](defaultValue)
  }

  //assumes the data is coalesced
  override protected def aggregateByChange(c: ChangeSpec, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): HybridGraph[VD, ED] = {
    //if we only have the structure, we can do efficient aggregation with the graph
    //otherwise just use the parent
    defaultValue match {
      case a: StructureOnlyAttr  => aggregateByChangeStructureOnly(c, vquant, equant)
      case _ => super.aggregateByChange(c, vquant, equant, vAggFunc, eAggFunc).asInstanceOf[HybridGraph[VD,ED]]
    }

  }
 
  private def aggregateByChangeStructureOnly(c: ChangeSpec, vquant: Quantification, equant: Quantification): HybridGraph[VD, ED] = {
    val size: Integer = c.num
    if (graphs.size < 1) computeGraphs()

    val grp = collectedIntervals.grouped(size).toList
    val countsSum = grp.map{ g => g.size }.scanLeft(0)(_ + _).tail
    val runsSum: Seq[Int] = widths.scanLeft(0)(_ + _).tail
    val newIntvs: Array[Interval] = collectedIntervals.grouped(size).map(grp => Interval(grp(0).start, grp.last.end)).toArray
    val newIntvsb = ProgramContext.sc.broadcast(newIntvs)
    val intvs = ProgramContext.sc.broadcast(collectedIntervals)

    var gps: ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = ParSeq[Graph[BitSet,(EdgeId,BitSet)]]()
    //there is no union of two graphs in graphx
    var firstVRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
    var firstERDD: RDD[Edge[(EdgeId,BitSet)]] = ProgramContext.sc.emptyRDD
    var startx:Int = 0
    var numagg:Int = 0
    var xx:Int = 0
    var yy:Int = 0
    var runs: Seq[Int] = Seq[Int]()

    while (xx < countsSum.size && yy < runsSum.size) {
      if (yy == (runsSum.size - 1) || countsSum(xx) == runsSum(yy)) {
        firstVRDD = firstVRDD.union(graphs(yy).vertices)
        firstERDD = firstERDD.union(graphs(yy).edges)
        numagg = numagg + 1

        if (yy == runsSum.size-1) xx = countsSum.size-1
        //we to go (p)-1 because counts are 1-based but indices are 0-based
        val parts:Seq[(Int,Int,Int)] = (startx to xx).map(p => (countsSum.lift(p-1).getOrElse(0), countsSum(p)-1, p))
        if (numagg > 1) {
          firstVRDD = firstVRDD.reduceByKey(_ ++ _)
          firstERDD = firstERDD.map{e => 
            ((e.attr._1, e.srcId, e.dstId), e.attr._2)}
            .reduceByKey(_ ++ _)
            .map(x => Edge(x._1._2, x._1._3, (x._1._1, x._2)))
        }
        gps = gps :+ Graph(firstVRDD.mapValues{ attr =>
          BitSet() ++ parts.flatMap { case (start, end, index) =>
            val mask = BitSet((start to end): _*)
            val tt = mask & attr
            if (tt.isEmpty)
              None
            else if (vquant.keep(tt.toList.map(ii => intvs.value(ii).ratio(newIntvsb.value(index))).reduce(_ + _)))
              Some(index)
            else
              None
          }
        }.filter{ case (vid, attr) => !attr.isEmpty}, EdgeRDD.fromEdges[(EdgeId,BitSet),BitSet](firstERDD).mapValues{e =>
          (e.attr._1, BitSet() ++ parts.flatMap { case (start, end, index) =>
            val mask = BitSet((start to end): _*)
            val tt = mask & e.attr._2
            if (tt.isEmpty)
              None
            else if (equant.keep(tt.toList.map(ii => intvs.value(ii).ratio(newIntvsb.value(index))).reduce(_ + _)))
              Some(index)
            else
              None
          })
        }.filter{ e => !e.attr._2.isEmpty},
          BitSet(), edgeStorageLevel = storageLevel,
          vertexStorageLevel = storageLevel)
          .mapTriplets(etp => (etp.attr._1, etp.srcAttr & etp.dstAttr & etp.attr._2))
          .subgraph(vpred = (vid, attr) => !attr.isEmpty, epred = e => !e.attr._2.isEmpty)

        //the number of snapshots in this new aggregate
        runs = runs :+ (xx - startx + 1)

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

    //collect vertices and edges
    val tmp: ED = defaultValue.asInstanceOf[ED]
    val vs = gps.map(g => g.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), defaultValue)))}).reduce(_ union _)
    val es = gps.map(g => g.edges.flatMap{ case e => e.attr._2.toSeq.map(ii => TEdge[ED](e.attr._1, e.srcId, e.dstId, newIntvsb.value(ii), tmp))}).reduce(_ union _)

    if (ProgramContext.eagerCoalesce)
      fromRDDs(vs, es, defaultValue, storageLevel, false)
    else
      new HybridGraph(newIntvs, vs, es, runs, gps, defaultValue, storageLevel, false)

  }

  override  protected def aggregateByTime(c: TimeSpec, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): HybridGraph[VD, ED] = {
    defaultValue match {
      case a: StructureOnlyAttr => aggregateByTimeStructureOnly(c, vquant, equant)
      case _ => super.aggregateByTime(c, vquant, equant, vAggFunc, eAggFunc).asInstanceOf[HybridGraph[VD, ED]]
    }
  }

  private def aggregateByTimeStructureOnly(c: TimeSpec, vquant: Quantification, equant: Quantification): HybridGraph[VD, ED] = {
    val start = span.start
    if (graphs.size < 1) computeGraphs()

    //make a mask which is a mapping of indices to new indices
    val newIntvs = span.split(c.res, start).map(_._2).reverse
    val indexed = collectedIntervals.zipWithIndex

    //for each index have a range of old indices from smallest to largest, inclusive
    val countSums = newIntvs.map{ intv =>
      val tmp = indexed.filter(ii => ii._1.intersects(intv))
      (tmp.head._2, tmp.last._2)
    }

    val runsSum: Seq[Int] = widths.scanLeft(0)(_ + _).tail
    val empty: Interval = Interval.empty
    val newIntvsb = ProgramContext.sc.broadcast(newIntvs)
    val intvs = ProgramContext.sc.broadcast(collectedIntervals)

    var gps: ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = ParSeq[Graph[BitSet,(EdgeId,BitSet)]]()
    //there is no union of two graphs in graphx
    var firstVRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
    var firstERDD: RDD[Edge[(EdgeId,BitSet)]] = ProgramContext.sc.emptyRDD
    var startx:Int = 0
    var numagg:Int = 0
    var xx:Int = 0
    var yy:Int = 0
    var runs: Seq[Int] = Seq[Int]()
    while (xx < countSums.size && yy < runsSum.size) {
      if (yy == (runsSum.size - 1) || countSums(xx)._2 == runsSum(yy)-1) {
        firstVRDD = firstVRDD.union(graphs(yy).vertices)
        firstERDD = firstERDD.union(graphs(yy).edges)
        numagg = numagg + 1

        if (yy == runsSum.size-1) xx = countSums.size-1
        val parts:Seq[(Int,Int,Int)] = (startx to xx).map(p => (countSums(p)._1, countSums(p)._2, p))
        if (numagg > 1) {
          firstVRDD = firstVRDD.reduceByKey(_ ++ _)
          firstERDD = firstERDD.map{e => ((e.attr._1, e.srcId, e.dstId), e.attr._2)}.reduceByKey(_ ++ _).map(x => Edge(x._1._2, x._1._3, (x._1._1,x._2)))
        }
        gps = gps :+ Graph(firstVRDD.mapValues{ attr =>
          BitSet() ++ parts.flatMap { case (start, end, index) =>
            val mask = BitSet((start to end): _*)
            val tt = mask & attr
            if (tt.isEmpty)
              None
            else if (vquant.keep(tt.toList.map(ii => intvs.value(ii).ratio(newIntvsb.value(index))).reduce(_ + _)))
              Some(index)
            else
              None
          }
        }.filter{ case (vid, attr) => !attr.isEmpty}, EdgeRDD.fromEdges[(EdgeId,BitSet),BitSet](firstERDD).mapValues{e =>
          (e.attr._1, BitSet() ++ parts.flatMap { case (start, end, index) =>
            val mask = BitSet((start to end): _*)
            val tt = mask & e.attr._2
            if (tt.isEmpty)
              None
            else if (equant.keep(tt.toList.map(ii => intvs.value(ii).ratio(newIntvsb.value(index))).reduce(_ + _)))
              Some(index)
            else
              None
          })
        }.filter{ e => !e.attr._2.isEmpty},
          BitSet(), edgeStorageLevel = storageLevel,
          vertexStorageLevel = storageLevel)
          .mapTriplets(etp => (etp.attr._1, etp.srcAttr & etp.dstAttr & etp.attr._2))
          .subgraph(vpred = (vid, attr) => !attr.isEmpty, epred = e => !e.attr._2.isEmpty)

        //the number of snapshots in this new aggregate
        runs = runs :+ (xx - startx + 1)
        //reset, move on
        firstVRDD = ProgramContext.sc.emptyRDD
        firstERDD = ProgramContext.sc.emptyRDD
        xx = xx+1
        yy = yy+1
        startx = xx
        numagg = 0
      } else if (countSums(xx)._2 < runsSum(yy)-1) {
        xx = xx+1
      } else { //runsSum(y) < countSums(x)
        firstVRDD = firstVRDD.union(graphs(yy).vertices)
        firstERDD = firstERDD.union(graphs(yy).edges)
        yy = yy+1
        numagg = numagg + 1
      }
    }

    //collect vertices and edges
    val tmp: ED = defaultValue.asInstanceOf[ED]
    val vs = gps.map(g => g.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), defaultValue)))}).reduce(_ union _)
    val es = gps.map(g => g.edges.flatMap{ case e => e.attr._2.toSeq.map(ii => TEdge[ED](e.attr._1, e.srcId, e.dstId, newIntvsb.value(ii), tmp))}).reduce(_ union _)

    if (ProgramContext.eagerCoalesce)
      fromRDDs(vs, es, defaultValue, storageLevel, false)
    else
      new HybridGraph(newIntvs.toArray, vs, es, runs, gps, defaultValue, storageLevel, false)

  }

  override def vmap[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2, defVal: VD2)(implicit eq: VD =:= VD2 = null): HybridGraph[VD2, ED] = {
    val vs = allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, map(vid, intv, attr)))}
    if (ProgramContext.eagerCoalesce)
      fromRDDs(vs, allEdges, defVal, storageLevel, false)
    else
      new HybridGraph(collectedIntervals, vs, allEdges, widths, graphs, defVal, storageLevel, false)
  }

  override def emap[ED2: ClassTag](map: TEdge[ED] => ED2): HybridGraph[VD, ED2] = {
    val es = allEdges.map{ te =>
      TEdge[ED2](te.eId, te.srcId, te.dstId, te.interval, map(te))}
    if (ProgramContext.eagerCoalesce)
      fromRDDs(allVertices, es, defaultValue, storageLevel, false)
    else
      new HybridGraph(collectedIntervals, allVertices, es, widths, graphs, defaultValue, storageLevel, false)
  }

  override def union(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): HybridGraph[VD,ED] = {
    defaultValue match {
      case a: StructureOnlyAttr => unionStructureOnly(other, vFunc, eFunc)
      case _ => super.union(other,vFunc,eFunc).asInstanceOf[HybridGraph[VD,ED]]
    }
  }

  private def unionStructureOnly(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): HybridGraph[VD,ED] = {
    var grp2: HybridGraph[VD, ED] = other match {
      case grph: HybridGraph[VD, ED] => grph
      case _ => return super.union(other,vFunc,eFunc).asInstanceOf[HybridGraph[VD,ED]]
    }

    if (graphs.size < 1) computeGraphs()
    if (grp2.graphs.size < 1) grp2.computeGraphs()

    //compute new intervals
    implicit val ord = dateOrdering
    val newIntvs: Array[Interval] = collectedIntervals.flatMap(ii => Seq(ii.start, ii.end)).union(grp2.collectedIntervals.flatMap(ii => Seq(ii.start, ii.end))).distinct.sortBy(c => c).sliding(2).map(x => Interval(x(0), x(1))).toArray
    val newIntvsb = ProgramContext.sc.broadcast(newIntvs)

    if (span.intersects(grp2.span)) {
      val zipped = newIntvs.zipWithIndex
      val intvMap: Map[Int, Seq[Int]] = collectedIntervals.zipWithIndex.map(ii => (ii._2, zipped.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMapB = ProgramContext.sc.broadcast(intvMap)
      val intvMap2: Map[Int, Seq[Int]] = grp2.collectedIntervals.zipWithIndex.map(ii => (ii._2, zipped.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMap2B = ProgramContext.sc.broadcast(intvMap2)

      //for each index in a bitset, put the new one
      val gp1: ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = graphs.map{g => g.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMapB.value(ii))
      }.mapEdges{ e =>
        (e.attr._1, BitSet() ++ e.attr._2.toSeq.flatMap(ii => intvMapB.value(ii)))
      }}
      val gp2: ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = grp2.graphs.map{g => g.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMap2B.value(ii))
      }.mapEdges{ e =>
        (e.attr._1, BitSet() ++ e.attr._2.toSeq.flatMap(ii => intvMap2B.value(ii)))
      }}

      val gr1IndexStart: Int = newIntvs.indexWhere(intv => intv.intersects(collectedIntervals.head))
      val gr2IndexStart: Int = newIntvs.indexWhere(intv => intv.intersects(grp2.collectedIntervals.head))
      val gr1Pad:Int = newIntvs.size - newIntvs.lastIndexWhere(intv => intv.intersects(collectedIntervals.head)) - 1
      val gr2Pad:Int = newIntvs.size - newIntvs.lastIndexWhere(intv => intv.intersects(grp2.collectedIntervals.head)) - 1

      val grseq1:ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = ParSeq.fill(gr1IndexStart){Graph[BitSet,(EdgeId,BitSet)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)} ++ gp1 ++ ParSeq.fill(gr1Pad){Graph[BitSet,(EdgeId,BitSet)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)}
      val grseq2:ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = ParSeq.fill(gr2IndexStart){Graph[BitSet,(EdgeId,BitSet)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)} ++ gp2 ++ ParSeq.fill(gr2Pad){Graph[BitSet,(EdgeId,BitSet)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)}

      val gr1Sums: Seq[Int] = (Seq.fill(gr1IndexStart){1} ++ (0 +: widths.scanLeft(0)(_+_).tail).sliding(2).map{x => (x.head to x.last-1).toList.map(y => intvMap(y).size).reduce(_+_)} ++ Seq.fill(gr1Pad){1}).scanLeft(0)(_ + _).tail
      val gr2Sums: Seq[Int] = (Seq.fill(gr2IndexStart){1} ++ (0 +: grp2.widths.scanLeft(0)(_+_).tail).sliding(2).map{x => (x.head to x.last-1).toList.map(y => intvMap2(y).size).reduce(_ + _)} ++ Seq.fill(gr2Pad){1}).scanLeft(0)(_+_).tail

      var gr1Index:Int = 0
      var gr2Index:Int = 0
      var runs: Seq[Int] = Seq[Int]()
      var gps: ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = ParSeq[Graph[BitSet,(EdgeId,BitSet)]]()
      //there is no union of two graphs in graphx
      var gr1VRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
      var gr1ERDD: RDD[Edge[(EdgeId,BitSet)]] = ProgramContext.sc.emptyRDD
      var gr2VRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
      var gr2ERDD: RDD[Edge[(EdgeId,BitSet)]] = ProgramContext.sc.emptyRDD
      //how many we have accumulated so far
      var gCount:Int = 0

      while (gr1Index < gr1Sums.size && gr2Index < gr2Sums.size) {
        //if aligned, union
        if (gr1Sums(gr1Index) == gr2Sums(gr2Index)) {
          gr1VRDD = gr1VRDD.union(grseq1(gr1Index).vertices)
          gr2VRDD = gr2VRDD.union(grseq2(gr2Index).vertices)
          gr1ERDD = gr1ERDD.union(grseq1(gr1Index).edges)
          gr2ERDD = gr2ERDD.union(grseq2(gr2Index).edges)

          gps = gps :+ Graph((gr1VRDD union gr2VRDD).reduceByKey(_ union _),
            (gr1ERDD union gr2ERDD).map(e => ((e.attr._1, e.srcId, e.dstId), e.attr._2))
              .reduceByKey(_ union _)
              .map{ case (k,v) => Edge(k._2, k._3, (k._1,v))}, 
            BitSet(), storageLevel, storageLevel)
          //how many graphs in this aggregated graphs
          runs = runs :+ (gr1Sums(gr1Index) - gCount)

          //done, now reset and keep going
          gCount = gr1Sums(gr1Index)
          gr1VRDD = ProgramContext.sc.emptyRDD
          gr2VRDD = ProgramContext.sc.emptyRDD
          gr1ERDD = ProgramContext.sc.emptyRDD
          gr2ERDD = ProgramContext.sc.emptyRDD
          gr1Index = gr1Index+1
          gr2Index = gr2Index+1
        } else if (gr1Sums(gr1Index) < gr2Sums(gr2Index)) { //accumulate graph1 groups
          gr1VRDD = gr1VRDD.union(grseq1(gr1Index).vertices)
          gr1ERDD = gr1ERDD.union(grseq1(gr1Index).edges)
          gr1Index = gr1Index+1
        } else { //gr2Sums(gr2Index) < gr1Sums(gr1Index)
          gr2VRDD = gr2VRDD.union(grseq2(gr2Index).vertices)
          gr2ERDD = gr2ERDD.union(grseq2(gr2Index).edges)
          gr2Index = gr2Index+1
        }
      }

      //collect vertices and edges
      val newDefVal = defaultValue
      val tmp =defaultValue.asInstanceOf[ED]
      val vs = gps.map(g => g.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), newDefVal)))}).reduce(_ union _)
      val es = gps.map(g => g.edges.flatMap{ case e => e.attr._2.toSeq.map(ii => TEdge[ED](e.attr._1, e.srcId, e.dstId, newIntvsb.value(ii), tmp))}).reduce(_ union _)

      if (ProgramContext.eagerCoalesce)
        fromRDDs(vs, es, newDefVal, storageLevel, false)
      else
        new HybridGraph(newIntvs, vs, es, runs, gps, newDefVal, storageLevel, false)

    } else {
      //like above, but no intervals are split, so reindexing is simpler
      //compute the starting index for each graph (with no overlap there aren't any multiples)
      val gr1IndexStart: Int = newIntvs.indexWhere(intv => intv.intersects(collectedIntervals.head))
      val gr2IndexStart: Int = newIntvs.indexWhere(intv => intv.intersects(grp2.collectedIntervals.head))

      val gp1 = if (gr1IndexStart > 0) graphs.map(g => g.mapVertices{ (vid, attr) =>
        attr.map(ii => ii + gr1IndexStart)
      }.mapEdges{ e =>
        (e.attr._1, e.attr._2.map(ii => ii + gr1IndexStart))
      }) else graphs
      val gp2 = if (gr2IndexStart > 0) grp2.graphs.map(g => g.mapVertices{ (vid, attr) =>
        attr.map(ii => ii + gr2IndexStart)
      }.mapEdges{ e =>
        (e.attr._1, e.attr._2.map(ii => ii + gr2IndexStart))
      }) else grp2.graphs

      //because there is no intersection, we can just put the sequences together
      val gps: ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = if (gr1IndexStart < gr2IndexStart) gp1 ++ gp2 else gp2 ++ gp1
      val runs: Seq[Int] = if (span.end == grp2.span.start || span.start == grp2.span.end) {
        if (gr1IndexStart < gr2IndexStart) widths ++ grp2.widths else grp2.widths ++ widths
      } else {
        if (gr1IndexStart < gr2IndexStart) widths ++ Seq(grp2.widths.head +1) ++ grp2.widths.tail else grp2.widths ++ Seq(widths.head +1) ++ widths.tail
      }

      //collect vertices and edges
      val newDefVal = defaultValue
      val tmp = defaultValue.asInstanceOf[ED]
      val vs = gps.map(g => g.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), newDefVal)))}).reduce(_ union _)
      val es = gps.map(g => g.edges.flatMap{ case e => e.attr._2.toSeq.map(ii => TEdge[ED](e.attr._1, e.srcId, e.dstId, newIntvsb.value(ii), tmp))}).reduce(_ union _)

      //whether the result is coalesced depends on whether the two inputs are coalesced and whether their spans meet
      val col = coalesced && grp2.coalesced && span.end != grp2.span.start && span.start != grp2.span.end
      if (ProgramContext.eagerCoalesce && !col)
        fromRDDs(vs, es, newDefVal, storageLevel, false)
      else
        new HybridGraph(newIntvs, vs, es, runs, gps, newDefVal, storageLevel, col)

    }
  }

  override def difference(other: TGraphNoSchema[VD, ED]): HybridGraph[VD,ED] = {
    defaultValue match {
      case a: StructureOnlyAttr => differenceStructureOnly(other)
      case _ => super.difference(other).asInstanceOf[HybridGraph[VD,ED]]
    }
  }

  def differenceStructureOnly(other: TGraphNoSchema[VD, ED]): HybridGraph[VD, ED] = {

    val grp2: HybridGraph[VD, ED] = other match {
      case grph: HybridGraph[VD, ED] => grph
      case _ => return super.difference(other).asInstanceOf[HybridGraph[VD,ED]]
    }

    if (span.intersects(grp2.span)) {
      if (graphs.size < 1) computeGraphs()
      if (grp2.graphs.size < 1) grp2.computeGraphs()

      //compute new intervals
      implicit val ord = dateOrdering
      val newIntvs: Array[Interval] = collectedIntervals.flatMap(ii => Seq(ii.start, ii.end)).union(grp2.collectedIntervals.flatMap(ii => Seq(ii.start, ii.end)).filter(ii => span.contains(ii))).distinct.sortBy(c => c).sliding(2).map(x => Interval(x(0), x(1))).toArray
      val newIntvsb = ProgramContext.sc.broadcast(newIntvs)

      val zipped = newIntvs.zipWithIndex
      val intvMap: Map[Int, Seq[Int]] = collectedIntervals.zipWithIndex.map(ii => (ii._2, zipped.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMapB = ProgramContext.sc.broadcast(intvMap)
      val intvMap2: Map[Int, Seq[Int]] = grp2.collectedIntervals.zipWithIndex.map(ii => (ii._2, zipped.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMap2B = ProgramContext.sc.broadcast(intvMap2)

      //for each index in a bitset, put the new one
      val gp1: ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = graphs.map{g => g.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMapB.value(ii))
      }.mapEdges{ e =>
        (e.attr._1, BitSet() ++ e.attr._2.toSeq.flatMap(ii => intvMapB.value(ii)))
      }}
      val gp2: ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = grp2.graphs.map{g => g.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMap2B.value(ii))
      }.mapEdges{ e =>
        (e.attr._1, BitSet() ++ e.attr._2.toSeq.flatMap(ii => intvMap2B.value(ii)))
      }}

      val gr1IndexStart: Int = newIntvs.indexWhere(intv => intv.intersects(collectedIntervals.head))
      val gr2IndexStart: Int = newIntvs.indexWhere(intv => intv.intersects(grp2.collectedIntervals.head))
      val gr1Pad:Int = newIntvs.size - newIntvs.lastIndexWhere(intv => intv.intersects(collectedIntervals.head)) - 1
      val gr2Pad:Int = newIntvs.size - newIntvs.lastIndexWhere(intv => intv.intersects(grp2.collectedIntervals.head)) - 1

      val grseq1:ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = ParSeq.fill(gr1IndexStart){Graph[BitSet,(EdgeId,BitSet)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)} ++ gp1 ++ ParSeq.fill(gr1Pad){Graph[BitSet,(EdgeId,BitSet)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)}
      val grseq2:ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = ParSeq.fill(gr2IndexStart){Graph[BitSet,(EdgeId,BitSet)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)} ++ gp2 ++ ParSeq.fill(gr2Pad){Graph[BitSet,(EdgeId,BitSet)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)}

      val gr1Sums: Seq[Int] = (Seq.fill(gr1IndexStart){1} ++ (0 +: widths.scanLeft(0)(_+_).tail).sliding(2).map{x => (x.head to x.last-1).toList.map(y => intvMap(y).size).reduce(_+_)} ++ Seq.fill(gr1Pad){1}).scanLeft(0)(_ + _).tail
      val gr2Sums: Seq[Int] = (Seq.fill(gr2IndexStart){1} ++ (0 +: grp2.widths.scanLeft(0)(_+_).tail).sliding(2).map{x => (x.head to x.last-1).toList.map(y => intvMap2(y).size).reduce(_ + _)} ++ Seq.fill(gr2Pad){1}).scanLeft(0)(_+_).tail

      var gr1Index:Int = 0
      var gr2Index:Int = 0
      var runs: Seq[Int] = Seq[Int]()
      var gps: ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = ParSeq[Graph[BitSet,(EdgeId,BitSet)]]()
      //there is no union of two graphs in graphx
      var gr1VRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
      var gr1ERDD: RDD[Edge[(EdgeId,BitSet)]] = ProgramContext.sc.emptyRDD
      var gr2VRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
      var gr2ERDD: RDD[Edge[(EdgeId,BitSet)]] = ProgramContext.sc.emptyRDD
      //how many we have accumulated so far
      var gCount:Int = 0

      while (gr1Index < gr1Sums.size && gr2Index < gr2Sums.size) {
        //if aligned, union
        if (gr1Sums(gr1Index) == gr2Sums(gr2Index)) {
          gr1VRDD = gr1VRDD.union(grseq1(gr1Index).vertices)
          gr2VRDD = gr2VRDD.union(grseq2(gr2Index).vertices)
          gr1ERDD = gr1ERDD.union(grseq1(gr1Index).edges)
          gr2ERDD = gr2ERDD.union(grseq2(gr2Index).edges)
          gps = gps :+ Graph((gr1VRDD.leftOuterJoin(gr2VRDD)).mapValues {x=> x._1.diff(x._2.getOrElse(BitSet()))}.filter( v =>  v._2.size>0),
              (gr1ERDD.map( e=>((e.attr._1, e.srcId,e.dstId),e.attr._2)).leftOuterJoin(gr2ERDD.map( e=>((e.attr._1, e.srcId,e.dstId),e.attr._2)))).mapValues(x=> x._1.diff((x._2.getOrElse(BitSet())))).filter( e => e._2.size>0).map(in=> Edge(in._1._2,in._1._3,(in._1._1,in._2))), BitSet(), storageLevel, storageLevel)
            .subgraph(vpred = (vid, attr) => !attr.isEmpty) //this will remove edges where vertices went away completely, automatically
            .mapTriplets( etp => (etp.attr._1, etp.attr._2 & etp.srcAttr & etp.dstAttr))
            .subgraph(epred = et => !et.attr._2.isEmpty)

            //how many graphs in this aggregated graphsg
          runs = runs :+ (gr1Sums(gr1Index) - gCount)

          //done, now reset and keep going
          gCount = gr1Sums(gr1Index)
          gr1VRDD = ProgramContext.sc.emptyRDD
          gr2VRDD = ProgramContext.sc.emptyRDD
          gr1ERDD = ProgramContext.sc.emptyRDD
          gr2ERDD = ProgramContext.sc.emptyRDD
          gr1Index = gr1Index+1
          gr2Index = gr2Index+1
        } else if (gr1Sums(gr1Index) < gr2Sums(gr2Index)) { //accumulate graph1 groups
          gr1VRDD = gr1VRDD.union(grseq1(gr1Index).vertices)
          gr1ERDD = gr1ERDD.union(grseq1(gr1Index).edges)
          gr1Index = gr1Index+1
        } else { //gr2Sums(gr2Index) < gr1Sums(gr1Index)
          gr2VRDD = gr2VRDD.union(grseq2(gr2Index).vertices)
          gr2ERDD = gr2ERDD.union(grseq2(gr2Index).edges)
          gr2Index = gr2Index+1
        }
      }

      //collect vertices and edges
      val newDefVal = (defaultValue.asInstanceOf[VD])
      val tmp = (defaultValue.asInstanceOf[ED])
      val vs = gps.map(g => g.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), newDefVal)))}).reduce(_ union _)
      val es = gps.map(g => g.edges.flatMap{ case e => e.attr._2.toSeq.map(ii => TEdge[ED](e.attr._1, e.srcId, e.dstId, newIntvsb.value(ii), tmp))}).reduce(_ union _)

      if (ProgramContext.eagerCoalesce)
        fromRDDs(vs, es, newDefVal, storageLevel, false)
      else
        new HybridGraph(newIntvs, vs, es, runs, gps, newDefVal, storageLevel, false)

    } else {
        this
    }
  }

  override def intersection(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): HybridGraph[VD, ED] = {
    defaultValue match {
      case a: StructureOnlyAttr => intersectionStructureOnly(other, vFunc, eFunc)
      case _ => super.intersection(other,vFunc,eFunc).asInstanceOf[HybridGraph[VD,ED]]
    }
  }

  private def intersectionStructureOnly(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): HybridGraph[VD,ED] = {
    val grp2: HybridGraph[VD, ED] = other match {
      case grph: HybridGraph[VD, ED] => grph
      case _ => return super.intersection(other,vFunc,eFunc).asInstanceOf[HybridGraph[VD,ED]]
    }

    if (span.intersects(grp2.span)) {
      if (graphs.size < 1) computeGraphs()
      if (grp2.graphs.size < 1) grp2.computeGraphs()

      //compute new intervals
      val st = maxDate(collectedIntervals.head.start, grp2.collectedIntervals.head.start)
      val en = minDate(collectedIntervals.last.end, grp2.collectedIntervals.last.end)
      val in = Interval(st, en)
      implicit val ord = dateOrdering
      val newIntvs: Array[Interval] = collectedIntervals.map(ii => ii.start).filter(ii => in.contains(ii)).union(grp2.collectedIntervals.map(ii => ii.start).filter(ii => in.contains(ii))).union(Seq(en)).distinct.sortBy(c => c).sliding(2).map(x => Interval(x(0), x(1))).toArray
      val newIntvsb = ProgramContext.sc.broadcast(newIntvs)
      val zipped = newIntvs.zipWithIndex

      val intvMap: Map[Int, Seq[Int]] = collectedIntervals.zipWithIndex.map(ii => (ii._2, zipped.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMapB = ProgramContext.sc.broadcast(intvMap)
      val intvMap2: Map[Int, Seq[Int]] = grp2.collectedIntervals.zipWithIndex.map(ii => (ii._2, zipped.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMap2B = ProgramContext.sc.broadcast(intvMap2)

      //for each index in a bitset, put the new one
      val grseq1: ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = graphs.map(g => g.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMapB.value(ii))
      }.subgraph(vpred = (vid, attr) => !attr.isEmpty)
        .mapEdges( e =>
        (e.attr._1, BitSet() ++ e.attr._2.toSeq.flatMap(ii => intvMapB.value(ii)))
      )
      ).filter(g => !g.vertices.isEmpty)
      val grseq2: ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = grp2.graphs.map(g => g.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMap2B.value(ii))
      }.subgraph(vpred = (vid, attr) => !attr.isEmpty)
        .mapEdges( e =>
        (e.attr._1, BitSet() ++ e.attr._2.toSeq.flatMap(ii => intvMap2B.value(ii)))
      )
      ).filter(g => !g.vertices.isEmpty)

      //compute new widths
      val gr1Sums: Seq[Int] = (0 +: widths.scanLeft(0)(_+_).tail).sliding(2).map{x => (x.head to x.last-1).toList.map(y => intvMap(y).size).reduce(_+_)}.dropWhile(_ == 0).toList.takeWhile(_ != 0).scanLeft(0)(_ + _).tail
      val gr2Sums: Seq[Int] = (0 +: grp2.widths.scanLeft(0)(_+_).tail).sliding(2).map{x => (x.head to x.last-1).toList.map(y => intvMap2(y).size).reduce(_+_)}.dropWhile(_ == 0).toList.takeWhile(_ != 0).scanLeft(0)(_+_).tail

      var gr1Index:Int = 0
      var gr2Index:Int = 0
      var runs: Seq[Int] = Seq[Int]()
      var gps: ParSeq[Graph[BitSet,(EdgeId,BitSet)]] = ParSeq[Graph[BitSet,(EdgeId,BitSet)]]()
      //there is no intersection of two graphs in graphx
      var gr1VRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
      var gr1ERDD: RDD[((EdgeId, VertexId, VertexId), BitSet)] = ProgramContext.sc.emptyRDD
      var gr2VRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
      var gr2ERDD: RDD[((EdgeId, VertexId, VertexId), BitSet)] = ProgramContext.sc.emptyRDD
      //how many we have accumulated so far
      var gCount:Int = 0
      var num1agg:Int = 0
      var num2agg:Int = 0

      while (gr1Index < gr1Sums.size && gr2Index < gr2Sums.size) {
        //if aligned, union
        if (gr1Sums(gr1Index) == gr2Sums(gr2Index)) {
          gr1VRDD = gr1VRDD.union(grseq1(gr1Index).vertices)
          gr2VRDD = gr2VRDD.union(grseq2(gr2Index).vertices)
          gr1ERDD = gr1ERDD.union(grseq1(gr1Index).edges.map(e => ((e.attr._1, e.srcId, e.dstId), e.attr._2)))
          gr2ERDD = gr2ERDD.union(grseq2(gr2Index).edges.map(e => ((e.attr._1, e.srcId, e.dstId), e.attr._2)))
          num1agg = num1agg + 1
          num2agg = num2agg + 1

          if (num1agg > 1) {
            gr1VRDD = gr1VRDD.reduceByKey(_ ++ _)
            gr1ERDD = gr1ERDD.reduceByKey(_ ++ _)
          }
          if (num2agg > 1) {
            gr2VRDD = gr2VRDD.reduceByKey(_ ++ _)
            gr2ERDD = gr2ERDD.reduceByKey(_ ++ _)
          }

          gps = gps :+ Graph((gr1VRDD join gr2VRDD).mapValues{ case (a,b) => a & b},
            (gr1ERDD join gr2ERDD).mapValues{ case (a,b) => a & b}.map{ case (k,v) => Edge(k._2, k._3, (k._1,v))}, BitSet(), storageLevel, storageLevel)
          //how many graphs in this aggregated graphs
          runs = runs :+ (gr1Sums(gr1Index) - gCount)

          //done, now reset and keep going
          gCount = gr1Sums(gr1Index)
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
          gr1ERDD = gr1ERDD.union(grseq1(gr1Index).edges.map(e => ((e.attr._1, e.srcId, e.dstId), e.attr._2)))
          gr1Index = gr1Index+1
          num1agg = num1agg + 1
        } else { //gr2Sums(gr2Index) < gr1Sums(gr1Index)
          gr2VRDD = gr2VRDD.union(grseq2(gr2Index).vertices)
          gr2ERDD = gr2ERDD.union(grseq2(gr2Index).edges.map(e => ((e.attr._1, e.srcId, e.dstId), e.attr._2)))
          gr2Index = gr2Index+1
          num2agg = num2agg + 1
        }
      }

      //collect vertices and edges
      val newDefVal = defaultValue
      val tmp = defaultValue.asInstanceOf[ED]
      val vs = gps.map(g => g.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), newDefVal)))}).reduce(_ union _)
      val es = gps.map(g => g.edges.flatMap{ case e => e.attr._2.toSeq.map(ii => TEdge[ED](e.attr._1, e.srcId, e.dstId, newIntvsb.value(ii), tmp))}).reduce(_ union _)

      //intersection of two coalesced structure-only graphs is not coalesced
      if (ProgramContext.eagerCoalesce)
        fromRDDs(vs, es, newDefVal, storageLevel, false)
      else
        new HybridGraph(newIntvs, vs, es, runs, gps, newDefVal, storageLevel, false)

    } else {
      emptyGraph(defaultValue)
    }

  }

  override def pregel[A: ClassTag]
  (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
    activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
    sendMsg: EdgeTriplet[VD, (EdgeId,ED)] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A): HybridGraph[VD, ED] = {
    throw new UnsupportedOperationException("pregel not yet implemented")
  }

  override def degree: RDD[(VertexId, (Interval, Int))] = {
    if (!allEdges.isEmpty) {
      if (graphs.size < 1) computeGraphs()

      val mergeFunc: (HashMap[TimeIndex,Int], HashMap[TimeIndex,Int]) => HashMap[TimeIndex,Int] = { case (a,b) =>
        a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
      }

      val degRDDs = graphs.map(g => g.aggregateMessages[HashMap[TimeIndex, Int]](
        ctx => {
          ctx.sendToSrc(HashMap[TimeIndex,Int]() ++ ctx.attr._2.seq.map(x => (x,1)))
          ctx.sendToDst(HashMap[TimeIndex,Int]() ++ ctx.attr._2.seq.map(x => (x,1)))
        },
        mergeFunc, TripletFields.None)
      )

      val intvs = ProgramContext.sc.broadcast(collectedIntervals)
      TGraphNoSchema.coalesce(degRDDs.reduce((x: RDD[(VertexId, HashMap[TimeIndex, Int])], y: RDD[(VertexId, HashMap[TimeIndex, Int])]) => x union y).flatMap{ case (vid, map) => map.toSeq.map{ case (k,v) => (vid, (intvs.value(k), v))}})
    } else
      ProgramContext.sc.emptyRDD
  }


  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): HybridGraph[(VD, Double), ED] = {
    if (graphs.size < 1) computeGraphs()
    val runSums = widths.scanLeft(0)(_ + _).tail

    val undirected = !uni

    val prank = (grp: Graph[BitSet,(EdgeId,BitSet)], minIndex: Int, maxIndex: Int) => {
      //if (grp.edges.isEmpty)
      //  ProgramContext.sc.emptyRDD[(VertexId, Map[TimeIndex, (Double,Double)])]
      //else {
        val mergeFunc = (a:Int2IntOpenHashMap, b:Int2IntOpenHashMap) => {
          val itr = a.iterator

          while(itr.hasNext){
            val (index, count) = itr.next()
            b.update(index, (count + b.getOrDefault(index, 0)))
          }
          b
        }

        val vertexProgram = (id: VertexId, attr: Int2ObjectOpenHashMap[(Double, Double)], msg: Int2DoubleOpenHashMap) => {
          val vals = attr.clone
          val itr = attr.iterator
          while (itr.hasNext) {
            val (index, x) = itr.next()
            val newPr = x._1 + (1.0 - resetProb) * msg.getOrDefault(index, 0)
            vals.update(index, (newPr, newPr-x._1))
          }
          vals
        }

        val sendMessage = if (undirected)
          (edge: EdgeTriplet[Int2ObjectOpenHashMap[(Double, Double)], Int2ObjectOpenHashMap[(Double, Double)]]) => {
          //need to generate an iterator of messages for each index
          edge.attr.toList.flatMap{ case (k,v) =>
            if (edge.srcAttr(k)._2 > tol && edge.dstAttr(k)._2 > tol) {
              Iterator((edge.dstId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.srcAttr(k)._2 * v._1))),
                (edge.srcId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.dstAttr(k)._2 * v._2))))
            } else if (edge.srcAttr(k)._2 > tol) {
              Some((edge.dstId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.srcAttr(k)._2 * v._1))))
            } else if (edge.dstAttr(k)._2 > tol) {
              Some((edge.srcId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.dstAttr(k)._2 * v._2))))
            } else {
              None
            }
          }
            .iterator
        }
        else
          (edge: EdgeTriplet[Int2ObjectOpenHashMap[(Double, Double)], Int2ObjectOpenHashMap[(Double, Double)]]) => {
            edge.attr.toList.flatMap{ case (k,v) =>
              if  (edge.srcAttr.apply(k)._2 > tol) {
                Some((edge.dstId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.srcAttr.apply(k)._2 * v._1))))
              } else {
                None
              }
            }
              .iterator
          }

        val messageCombiner = (a: Int2DoubleOpenHashMap, b: Int2DoubleOpenHashMap) => {
          val itr = a.iterator

          while(itr.hasNext){
            val (index, count) = itr.next()
            b.update(index, (count + b.getOrDefault(index,0.0)))
          }
          b
        }

        val degs: VertexRDD[Int2IntOpenHashMap] = grp.aggregateMessages[Int2IntOpenHashMap](
          ctx => {
            ctx.sendToSrc{new Int2IntOpenHashMap(ctx.attr._2.toArray, Array.fill(ctx.attr._2.size)(1))}
           if (undirected) ctx.sendToDst{new Int2IntOpenHashMap(ctx.attr._2.toArray, Array.fill(ctx.attr._2.size)(1))} else ctx.sendToDst{new Int2IntOpenHashMap(ctx.attr._2.toArray, Array.fill(ctx.attr._2.size)(0))}
          },
          mergeFunc, TripletFields.None)

        val prankGraph: Graph[Int2ObjectOpenHashMap[(Double, Double)], Int2ObjectOpenHashMap[(Double, Double)]] = grp.outerJoinVertices(degs) {
          case (vid, vdata, Some(deg)) => deg
          case (vid, vdata, None) => new Int2IntOpenHashMap()
          }
          .mapTriplets{ e:EdgeTriplet[Int2IntOpenHashMap, (EdgeId,BitSet)] => new Int2ObjectOpenHashMap[(Double, Double)](e.attr._2.toArray, e.attr._2.toArray.map(x => (1.0/e.srcAttr(x), 1.0/e.dstAttr(x))))}
          .mapVertices( (id,attr) => new Int2ObjectOpenHashMap[(Double, Double)](attr.keySet().toIntArray, Array.fill(attr.size)((0.0,0.0)))).cache()

        val initialMessage: Int2DoubleOpenHashMap = new Int2DoubleOpenHashMap((minIndex to maxIndex).toArray, Array.fill(maxIndex-minIndex+1)(resetProb/(1.0-resetProb)))

        val dir = if (undirected) EdgeDirection.Either else EdgeDirection.Out
        Pregel(prankGraph, initialMessage, numIter, activeDirection = dir)(vertexProgram, sendMessage, messageCombiner)
          .asInstanceOf[Graph[Map[TimeIndex,(Double,Double)], Map[TimeIndex,(Double,Double)]]].vertices
      //}
    }
  
    val allgs:ParSeq[RDD[(VertexId, Map[TimeIndex,(Double,Double)])]] = graphs.zipWithIndex.map{ case (g,i) => prank(g, runSums.lift(i-1).getOrElse(0), runSums(i))}

    //now extract values
    val zipped = ProgramContext.sc.broadcast(collectedIntervals.zipWithIndex)
    val vattrs= allgs.reduce(_ union _).reduceByKey((a,b) => (a ++ b))
    //now need to join with the previous value
    val newverts = allVertices.leftOuterJoin(vattrs).flatMap{
      case (vid, (vdata, Some(prank))) =>
        zipped.value.filter(ii => ii._1.intersects(vdata._1)).map(ii => (vid, (ii._1, (vdata._2, prank.getOrDefault(ii._2, (resetProb, 0.0))._1))))
      case (vid, (vdata, None)) => Some((vid, (vdata._1, (vdata._2, resetProb))))
    }

    if (ProgramContext.eagerCoalesce)
      fromRDDs(newverts, allEdges, (defaultValue, 0.0), storageLevel, false)
    else
      new HybridGraph(collectedIntervals, newverts, allEdges, widths, graphs, (defaultValue, 0.0), storageLevel, false)
  }

  override def connectedComponents(): HybridGraph[(VD, VertexId), ED] = {
    if (graphs.size < 1) computeGraphs()
    val runSums = widths.scanLeft(0)(_ + _).tail

    val conc = (grp: Graph[BitSet,(EdgeId,BitSet)], minIndex: Int, maxIndex: Int) => {
      val conGraph: Graph[Int2LongOpenHashMap, (EdgeId,BitSet)]
      = grp.mapVertices{ case (vid, bset) => new Int2LongOpenHashMap()}

      val vertexProgram = (id: VertexId, attr: Int2LongOpenHashMap, msg: Int2LongOpenHashMap) => {
        var vals = attr.clone()

        msg.foreach { x =>
          val (k,v) = x
          vals.update(k, math.min(v, attr.getOrDefault(k, id)))
        }
        vals
      }

      val sendMessage = (edge: EdgeTriplet[Int2LongOpenHashMap, (EdgeId,BitSet)]) => {
        edge.attr._2.toList.flatMap{ k =>
          if (edge.srcAttr.getOrDefault(k, edge.srcId) < edge.dstAttr.getOrDefault(k, edge.dstId))
            Some((edge.dstId, new Int2LongOpenHashMap(Array(k), Array(edge.srcAttr.getOrDefault(k, edge.srcId).toLong))))
          else if (edge.srcAttr.getOrDefault(k, edge.srcId) > edge.dstAttr.getOrDefault(k, edge.dstId))
            Some((edge.srcId, new Int2LongOpenHashMap(Array(k), Array(edge.dstAttr.getOrDefault(k, edge.dstId).toLong))))
          else
            None
        }
	  .iterator
      }

      val messageCombiner = (a: Int2LongOpenHashMap, b: Int2LongOpenHashMap) => {
        val itr = a.iterator

        while(itr.hasNext){
          val (index, minid) = itr.next()
          b.put(index: Int, math.min(minid, b.getOrDefault(index, Long.MaxValue)))
        }
        b
      }

      //there is really no reason to send an initial message
      val initialMessage: Int2LongOpenHashMap = new Int2LongOpenHashMap()

      Pregel(conGraph, initialMessage, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner).asInstanceOf[Graph[Map[TimeIndex, VertexId], (EdgeId,BitSet)]].vertices
    }

    val allgs:ParSeq[RDD[(VertexId, Map[TimeIndex, Long])]] = graphs.zipWithIndex.map{ case (g,i) => conc(g, runSums.lift(i-1).getOrElse(0), runSums(i))}

    //now extract values
    val zipped = ProgramContext.sc.broadcast(collectedIntervals.zipWithIndex)
    val vattrs= allgs.reduce(_ union _).reduceByKey((a,b) => (a ++ b))
    //now need to join with the previous value
    val newverts = allVertices.leftOuterJoin(vattrs).flatMap{
      case (vid, (vdata, Some(cc))) =>
        zipped.value.filter(ii => ii._1.intersects(vdata._1)).map(ii => (vid, (ii._1, (vdata._2, cc.getOrDefault(ii._2, vid)))))
      case (vid, (vdata, None)) => Some((vid, (vdata._1, (vdata._2, vid))))
    }

    if (ProgramContext.eagerCoalesce)
      fromRDDs(newverts, allEdges, (defaultValue, -1L), storageLevel, false)
    else
      new HybridGraph(collectedIntervals, newverts, allEdges, widths, graphs, (defaultValue, -1L), storageLevel, false)
  }

  override def shortestPaths(uni: Boolean, landmarks: Seq[VertexId]): HybridGraph[(VD, Map[VertexId, Int]), ED] = {
    if (graphs.size < 1) computeGraphs()
    val runSums = widths.scanLeft(0)(_ + _).tail

    val paths = (grp: Graph[BitSet,(EdgeId,BitSet)], minIndex: Int, maxIndex: Int) => {
      val makeMap = (x: Seq[(VertexId, Int)]) => {
        //we have to make a new map instead of modifying the input
        //because that has unintended consequences
        new Long2IntOpenHashMap(x.map(_._1).toArray, x.map(_._2).toArray)
      }

      val incrementMap = (spmap: Long2IntOpenHashMap) => {
        //we have to make a new map instead of modifying the input
        //because that has unintended consequences
        val itr = spmap.iterator
        val tmpMap = new Long2IntOpenHashMap()

        while (itr.hasNext) {
          val(k,v) = itr.next()
          tmpMap.put(k: Long, v+1)
        }
        tmpMap
      }

      val addMaps = (spmap1: Long2IntOpenHashMap, spmap2:Long2IntOpenHashMap) => {
        val itr = spmap1.iterator
        val vals = spmap2.clone

        while (itr.hasNext) {
          val (index, oldv) = itr.next()
          vals.update(index, math.min(oldv, spmap2.getOrDefault(index, Int.MaxValue)))
        }
        vals
      }

      val spGraph: Graph[Int2ObjectOpenHashMap[Long2IntOpenHashMap], (EdgeId,BitSet)] = grp.mapVertices { (vid, attr) =>
        // Set the vertex attributes to vertex id for each interval
        if (landmarks.contains(vid)) {
          new Int2ObjectOpenHashMap[Long2IntOpenHashMap](attr.toArray, Array.fill(attr.size)(makeMap(Seq(vid -> 0))))
        } else new Int2ObjectOpenHashMap[Long2IntOpenHashMap]()
      }

      val initialMessage: Int2ObjectOpenHashMap[Long2IntOpenHashMap] =
        new Int2ObjectOpenHashMap[Long2IntOpenHashMap]()

      val addMapsCombined = (a: Int2ObjectOpenHashMap[Long2IntOpenHashMap], b: Int2ObjectOpenHashMap[Long2IntOpenHashMap]) => {
        val itr = a.iterator

        while(itr.hasNext){
          val(index, mp) = itr.next()
          b.put(index.toInt, addMaps(mp, b.getOrElse(index, makeMap(Seq[(VertexId,Int)]()))))
        }
        b
      }

      val vertexProgram = (id: VertexId, attr: Int2ObjectOpenHashMap[Long2IntOpenHashMap], msg: Int2ObjectOpenHashMap[Long2IntOpenHashMap]) => {
        //need to compute new shortestPaths to landmark for each interval
        //each edge carries a message for one interval,
        //which are combined by the combiner into a hash
        //for each interval in the msg hash, update
        val vals = attr.clone

        msg.foreach { x =>
          val (k, v) = x
          vals.update(k, addMaps(attr.getOrDefault(k, makeMap(Seq[(VertexId,Int)]())), v))
        }

        vals
      }

      val sendMessage = if (uni)
        (edge: EdgeTriplet[Int2ObjectOpenHashMap[Long2IntOpenHashMap], (EdgeId,BitSet)]) => {
          //each vertex attribute is supposed to be a map of int->spmap
          edge.attr._2.toList.flatMap { k =>
            val srcSpMap = edge.srcAttr.getOrDefault(k, makeMap(Seq[(VertexId,Int)]()))
            val dstSpMap = edge.dstAttr.getOrDefault(k, makeMap(Seq[(VertexId,Int)]()))
            val newAttr = incrementMap(dstSpMap)

            if (srcSpMap != addMaps(newAttr, srcSpMap))
              Some((edge.srcId, new Int2ObjectOpenHashMap[Long2IntOpenHashMap](Array(k), Array(newAttr))))
            else
              None
          }
            .iterator
        }
        else
          (edge: EdgeTriplet[Int2ObjectOpenHashMap[Long2IntOpenHashMap], (EdgeId,BitSet)]) => {
            //each vertex attribute is supposed to be a map of int->spmap for each index
            edge.attr._2.toList.flatMap{ k =>
              val srcSpMap = edge.srcAttr.getOrDefault(k, makeMap(Seq[(VertexId,Int)]()))
              val dstSpMap = edge.dstAttr.getOrDefault(k, makeMap(Seq[(VertexId,Int)]()))
              val newAttr = incrementMap(dstSpMap)
              val newAttr2 = incrementMap(srcSpMap)

              if (srcSpMap != addMaps(newAttr, srcSpMap)) {
                Some((edge.srcId, new Int2ObjectOpenHashMap[Long2IntOpenHashMap](Array(k), Array(newAttr))))
              } else if (dstSpMap != addMaps(newAttr2, dstSpMap)) {
                Some((edge.dstId, new Int2ObjectOpenHashMap[Long2IntOpenHashMap](Array(k), Array(newAttr2))))
              } else
                None
            }
              .iterator
          }

      Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMapsCombined).asInstanceOf[Graph[Map[TimeIndex,Long2IntOpenHashMap],(EdgeId,BitSet)]].vertices
    }

    val allgs:ParSeq[RDD[(VertexId, Map[TimeIndex, Long2IntOpenHashMap])]] = graphs.zipWithIndex.map{ case (g,i) => paths(g, runSums.lift(i-1).getOrElse(0), runSums(i))}

    //now extract values
    val zipped = ProgramContext.sc.broadcast(collectedIntervals.zipWithIndex)
    val vattrs= allgs.reduce(_ union _).reduceByKey((a,b) => (a ++ b))
    //now need to join with the previous value
    val emptyMap = new Long2IntOpenHashMap()
    val newverts = allVertices.leftOuterJoin(vattrs).flatMap{
      case (vid, (vdata, Some(sp))) =>
        zipped.value.filter(ii => ii._1.intersects(vdata._1)).map(ii => (vid, (ii._1, (vdata._2, sp.getOrDefault(ii._2, emptyMap).asInstanceOf[Map[VertexId,Int]]))))
      case (vid, (vdata, None)) => Some((vid, (vdata._1, (vdata._2, emptyMap.asInstanceOf[Map[VertexId,Int]]))))
    }

    if (ProgramContext.eagerCoalesce)
      fromRDDs(newverts, allEdges, (defaultValue, emptyMap.asInstanceOf[Map[VertexId,Int]]), storageLevel, false)
    else
      new HybridGraph(collectedIntervals, newverts, allEdges, widths, graphs, (defaultValue, emptyMap.asInstanceOf[Map[VertexId,Int]]), storageLevel, false)

  }

  override def aggregateMessages[A: ClassTag](sendMsg: TEdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A, defVal: A, tripletFields: TripletFields = TripletFields.All): HybridGraph[(VD, A), ED] = {
    if (graphs.size < 1) computeGraphs()
    if (tripletFields != TripletFields.None) {
      super.aggregateMessages(sendMsg,mergeMsg,defVal,tripletFields).asInstanceOf[HybridGraph[(VD,A),ED]]
    }
    else {
      val aggMap = (grp: Graph[BitSet, (EdgeId,BitSet)]) => {
        grp.aggregateMessages[Int2ObjectOpenHashMap[A]](
          ctx => {
            val edge = ctx.toEdgeTriplet
            val triplet = new TEdgeTriplet[VD, ED]
            triplet.eId = edge.attr._1
            triplet.srcId = edge.srcId
            triplet.dstId = edge.dstId
            sendMsg(triplet).foreach { x =>
              val tmp = new Int2ObjectOpenHashMap[A]()
              ctx.attr._2.seq.foreach { index =>
                tmp.put(index, x._2)
              }
              if (x._1 == edge.srcId)
                ctx.sendToSrc(tmp)
              else if (x._1 == edge.dstId)
                ctx.sendToDst(tmp)
              else
                throw new IllegalArgumentException("trying to send message to a vertex that is neither a source nor a destination")
            }
          },
          (a, b) => {
            val itr = a.iterator
            while (itr.hasNext) {
              val (index, vl) = itr.next()
              b.update(index, mergeMsg(vl, b.getOrElse(index, defVal)))
            }
            b
          }, tripletFields)
      }

      val allgs = graphs.zipWithIndex.map { case (g, i) => aggMap(g) }
      //now extract values
      val vattrs: RDD[(VertexId, Int2ObjectOpenHashMap[A])] = allgs.reduce((a: RDD[(VertexId, Int2ObjectOpenHashMap[A])], b: RDD[(VertexId, Int2ObjectOpenHashMap[A])]) => a union b).reduceByKey((a,b) => (a ++ b).asInstanceOf[Int2ObjectOpenHashMap[A]])
      val zipped = ProgramContext.sc.broadcast(collectedIntervals.zipWithIndex)
      //now need to join with the previous value
      val newverts = allVertices.leftOuterJoin(vattrs).flatMap{
        case (vid, (vdata, Some(msg))) =>
          zipped.value.filter(ii => ii._1.intersects(vdata._1)).map(ii => (vid, (ii._1, (vdata._2, msg.getOrDefault(ii._2, defVal)))))
        case (vid, (vdata, None)) => Some((vid, (vdata._1, (vdata._2, defVal))))
      }

      if (ProgramContext.eagerCoalesce)
        fromRDDs(newverts, allEdges, (defaultValue, defVal), storageLevel, false)
      else
        new HybridGraph(collectedIntervals, newverts, allEdges, widths, graphs, (defaultValue, defVal), storageLevel, false)
    }
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    if (graphs.size < 1) computeGraphs()
    graphs.filterNot(_.edges.isEmpty).map(_.edges.getNumPartitions).reduce(_ + _)
  }

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): HybridGraph[VD, ED] = {
    super.persist(newLevel)
    //persist each graph if it is not yet persisted
    graphs.map(g => g.persist(newLevel))
    this
  }

  override def unpersist(blocking: Boolean = true): HybridGraph[VD, ED] = {
    super.unpersist(blocking)
    graphs.map(_.unpersist(blocking))
    this
  }
  
  override def partitionBy(tgp: TGraphPartitioning): HybridGraph[VD, ED] = {
    if (tgp.pst != PartitionStrategyType.None) {
      partitioning = tgp

      if (graphs.size > 0) {
        val count = collectedIntervals.size
        new HybridGraph(collectedIntervals, allVertices, allEdges, widths, graphs.zipWithIndex.map { case (g,index) =>
          val numParts: Int = if (tgp.parts > 0) tgp.parts else g.edges.getNumPartitions
          g.partitionBy(PartitionStrategies.makeStrategy(tgp.pst, index, count, tgp.runs), numParts)}, defaultValue, storageLevel, coalesced)
      } else this
    } else
      this
  }

  override def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[TEdge[E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false): HybridGraph[V, E] = {
    HybridGraph.fromRDDs(verts, edgs, defVal, storLevel, coalesced = coal)
  }

  override protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): HybridGraph[V, E] = HybridGraph.emptyGraph(defVal)

  protected def computeGraphs(runWidth: Int = 8): Unit = {
    val zipped = ProgramContext.sc.broadcast(collectedIntervals.toList.zipWithIndex)
    val split: (Interval => BitSet) = (interval: Interval) => {
      BitSet() ++ zipped.value.flatMap( ii => if (interval.intersects(ii._1)) Some(ii._2) else None)
    }

    val count = collectedIntervals.size
    val vertsConverted = allVertices.mapValues{ case (intv, attr) => split(intv)}.cache()
    val edgesConverted = allEdges.map{e => ((e.eId, e.srcId, e.dstId), split(e.interval))}.cache()
    val combined = (0 to (count-1)).grouped(runWidth).map { intvs =>
      val set = BitSet() ++ intvs
      (intvs.size,
        Graph(vertsConverted.mapValues(v => set & v)
	       .filter(v => !v._2.isEmpty)
               .reduceByKey((a,b) => a union b),
          edgesConverted.mapValues(e => set & e)
              .filter(e => !e._2.isEmpty)
              .reduceByKey((a,b) => a union b)
              .map(e => Edge(e._1._2, e._1._3, (e._1._1,e._2))),
          BitSet(), storLevel, storLevel)
      )}.toList

    widths = combined.map(x => x._1)
    graphs = combined.map(x => x._2).par

    if (partitioning.pst != PartitionStrategyType.None) {
      graphs = graphs.zipWithIndex.map { case (g, index) =>
        val numParts: Int = if (partitioning.parts > 0) partitioning.parts else g.edges.getNumPartitions
        g.partitionBy(PartitionStrategies.makeStrategy(partitioning.pst, index, count, partitioning.runs), numParts)}
    }
  }

}

object HybridGraph extends Serializable {
  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): HybridGraph[V, E] = new HybridGraph(Array[Interval](), ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, Seq[Int](), ParSeq[Graph[BitSet,(EdgeId,BitSet)]](), defVal, coal = true)

  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[TEdge[E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): HybridGraph[V, E] = {
    val cverts = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(verts) else verts
    val cedges = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(edgs.map(_.toPaired)).map(e => TEdge(e._1,e._2)) else edgs
    val coal = coalesced | ProgramContext.eagerCoalesce

    val intervals = TGraphNoSchema.computeIntervals(cverts, cedges).collect
    //because we use "lazy" evaluation, we don't compute the graphs until we need them
    new HybridGraph(intervals, cverts, cedges, Seq[Int](), ParSeq[Graph[BitSet,(EdgeId,BitSet)]](), defVal, storLevel, coal)
  }

  def fromDataFrames[V: ClassTag, E: ClassTag](verts: Array[org.apache.spark.sql.DataFrame], edgs: Array[org.apache.spark.sql.DataFrame], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): HybridGraph[V, E] = {
    //TODO: check for eagerCoalesce and coalesce if needed

    //we want to make a separate graph from each snapshot group
    val intervals: Array[Array[Interval]] = verts.zip(edgs).map{ case (vs, es) =>
      //compute the intervals
      vs.select("estart", "eend").rdd.flatMap(r => Seq(r.getLong(0), r.getLong(1))).union(es.select("estart", "eend").rdd.flatMap(r => Seq(r.getLong(0), r.getLong(1)))).distinct.collect.sortBy(c => c).sliding(2).map(lst => Interval(lst(0), lst(1))).toArray
    }

    val intervalList = intervals.flatten //this should preserve the order
    val widths = intervals.map(ii => ii.size)
    val partialSum: Seq[Int] = widths.scanLeft(0)(_ + _).tail

    val zipped = ProgramContext.sc.broadcast(intervalList.toList.map(ii => (ii.getStartSeconds, ii.getEndSeconds)).zipWithIndex)
    val split: ((Long, Long) => BitSet) = (st: Long, en: Long) => {
       BitSet() ++ zipped.value.flatMap(ii => if (en > ii._1._1 && st < ii._1._2) Some(ii._2) else None) //None else Some(ii._2))
    }

    val graphs = verts.zip(edgs).zipWithIndex.map{ case ((vs, es), index) =>
      //take all the vertices in this dataframe, convert to bitset
      Graph(vs.rdd.map(r => (r.getLong(0), split(r.getLong(1), r.getLong(2)))).reduceByKey((a,b) => a union b),
        es.rdd.map(r => ((r.getLong(0), r.getLong(1), r.getLong(2)), split(r.getLong(3), r.getLong(4))))
          .reduceByKey((a,b) => a union b)
          .map(e => Edge(e._1._2, e._1._3, (e._1._1,e._2))),
        BitSet(), storLevel, storLevel)
    }.par

    val cverts: RDD[(VertexId, (Interval, V))] = verts.map(vv => vv.rdd.map(r => (r.getLong(0), (Interval(r.getLong(1), r.getLong(2)), r.getAs[V](3))))).reduce((a,b) => a union b)
    val ceds = edgs.map(ee => ee.rdd.map(r => ((r.getLong(0), r.getLong(1), r.getLong(2)), (Interval(r.getLong(3), r.getLong(4)), r.getAs[E](5))))).reduce((a,b) => a union b)

    new HybridGraph(intervalList, cverts, ceds.map(e => TEdge(e._1, e._2)), widths, graphs, defVal, storLevel, coalesced)
  }

}
