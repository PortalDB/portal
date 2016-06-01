package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.parallel.ParSeq
import scala.collection.immutable.BitSet
import scala.collection.mutable.HashMap
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
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps._

import java.time.LocalDate

class HybridGraph[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[((VertexId, VertexId), (Interval, ED))], runs: Seq[Int], gps: ParSeq[Graph[BitSet, BitSet]], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraphNoSchema[VD, ED](intvs, verts, edgs, defValue, storLevel, coal) with Serializable {

  val graphs: ParSeq[Graph[BitSet, BitSet]] = gps
  //this is how many consecutive intervals are in each aggregated graph
  val widths: Seq[Int] = runs

  if (widths.size > 0 && intvs.size > 0) {
    if (widths.reduce(_ + _) != intvs.size)
      throw new IllegalArgumentException("temporal sequence and runs do not match: " + intvs.size + ", widthssum: " + widths.reduce(_+_))
  } else if (!(widths.size == 0 && intvs.size == 0)) {
    throw new IllegalArgumentException("temporal sequence and runs do not match")
  }

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

  /** Query operations */
  
  override def slice(bound: Interval): HybridGraph[VD, ED] = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this

    if (span.intersects(bound)) {
      val startBound = maxDate(span.start, bound.start)
      val endBound = minDate(span.end, bound.end)
      val selectBound:Interval = Interval(startBound, endBound)
      val start = span.start

      //compute indices of start and stop
      //start and stop both inclusive
      val selectStart:Int = intervals.indexWhere(intv => intv.intersects(selectBound))
      var selectStop:Int = intervals.lastIndexWhere(intv => intv.intersects(selectBound))
      if (selectStop < 0) selectStop = intervals.size-1
      val newIntvs: Seq[Interval] = intervals.slice(selectStart, selectStop+1)

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
      val subg:ParSeq[Graph[BitSet,BitSet]] = graphs.zipWithIndex.flatMap{ case (g,index) =>
        if (index == indexStart || index == indexStop) {
          val mask: BitSet = if (index == indexStart) BitSet((selectStart to stop1): _*) else BitSet((stop2 to selectStop): _*)
          Some(g.subgraph(
            vpred = (vid, attr) => !(attr & mask).isEmpty,
            epred = et => !(et.attr & mask).isEmpty)
            .mapVertices((vid, vattr) => vattr.filter(x => x >= selectStart && x <= selectStop).map(_ - selectStart)).mapEdges(e => e.attr.filter(x => x >= selectStart && x <= selectStop).map(_ - selectStart)))
        } else if (index > indexStart && index < indexStop) {
          Some(g.mapVertices((vid, vattr) => vattr.map(_ - selectStart))
            .mapEdges(e => e.attr.map(_ - selectStart)))
        } else
          None
      }

      //now need to update the vertex attribute rdd and edge attr rdd
      val vattrs = allVertices.filter{ case (k,v) => v._1.intersects(selectBound)}.mapValues( v => (Interval(maxDate(v._1.start, startBound), minDate(v._1.end, endBound)), v._2))
      val eattrs = allEdges.filter{ case (k,v) => v._1.intersects(selectBound)}.mapValues( v => (Interval(maxDate(v._1.start, startBound), minDate(v._1.end, endBound)), v._2))

      new HybridGraph[VD, ED](newIntvs, vattrs, eattrs, runs, subg, defaultValue, storageLevel, coalesced)

    } else
      HybridGraph.emptyGraph[VD,ED](defaultValue)
  }

  //assumes the data is coalesced
  override protected def aggregateByChange(c: ChangeSpec, vgroupby: (VertexId, VD) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): HybridGraph[VD, ED] = {
    //if we only have the structure, we can do efficient aggregation with the graph
    //otherwise just use the parent
    //FIXME: find a better way to tell there's only structure
    defaultValue match {
      case null if (vgroupby == vgb) => aggregateByChangeStructureOnly(c, vquant, equant)
      case _ => super.aggregateByChange(c, vgroupby, vquant, equant, vAggFunc, eAggFunc).asInstanceOf[HybridGraph[VD,ED]]
    }
  }
 
  private def aggregateByChangeStructureOnly(c: ChangeSpec, vquant: Quantification, equant: Quantification): HybridGraph[VD, ED] = {
    val size: Integer = c.num
    val grp = intervals.grouped(size).toList
    val countsSum = grp.map{ g => g.size }.scanLeft(0)(_ + _).tail
    val runsSum: Seq[Int] = widths.scanLeft(0)(_ + _).tail
    val newIntvs = grp.map{ grp => grp.reduce((a,b) => Interval(a.start, b.end))}
    val newIntvsb = ProgramContext.sc.broadcast(newIntvs)
    val intvs = ProgramContext.sc.broadcast(intervals)

    var gps: ParSeq[Graph[BitSet,BitSet]] = ParSeq[Graph[BitSet,BitSet]]()
    //there is no union of two graphs in graphx
    var firstVRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
    var firstERDD: RDD[Edge[BitSet]] = ProgramContext.sc.emptyRDD
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

        //we to go (p)-1 because counts are 1-based but indices are 0-based
        val parts:Seq[(Int,Int,Int)] = (startx to xx).map(p => (countsSum.lift(p-1).getOrElse(0), countsSum(p)-1, p))
        if (numagg > 1) {
          firstVRDD = firstVRDD.reduceByKey(_ ++ _)
          firstERDD = firstERDD.map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey(_ ++ _).map(x => Edge(x._1._1, x._1._2, x._2))
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
        }.filter{ case (vid, attr) => !attr.isEmpty}, EdgeRDD.fromEdges[BitSet,BitSet](firstERDD).mapValues{e =>
          BitSet() ++ parts.flatMap { case (start, end, index) =>
            val mask = BitSet((start to end): _*)
            val tt = mask & e.attr
            if (tt.isEmpty)
              None
            else if (equant.keep(tt.toList.map(ii => intvs.value(ii).ratio(newIntvsb.value(index))).reduce(_ + _)))
              Some(index)
            else
              None
          }
        }.filter{ e => !e.attr.isEmpty},
          BitSet(), edgeStorageLevel = storageLevel,
          vertexStorageLevel = storageLevel)
          .mapTriplets(etp => etp.srcAttr & etp.dstAttr & etp.attr)
          .subgraph(vpred = (vid, attr) => !attr.isEmpty)

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
    val tmp: ED = new Array[ED](1)(0)
    val vs = gps.map(g => g.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), defaultValue)))}).reduce(_ union _)
    val es = gps.map(g => g.edges.flatMap{ case e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp)))}).reduce(_ union _)

    new HybridGraph(newIntvs, vs, es, runs, gps, defaultValue, storageLevel, false)

  }

  override def union(other: TGraph[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): HybridGraph[VD, ED] = {
    defaultValue match {
      case null => unionStructureOnly(other)
      case _ => super.union(other, vFunc, eFunc).asInstanceOf[HybridGraph[VD,ED]]
    }
  }

  private def unionStructureOnly(other: TGraph[VD, ED]): HybridGraph[VD, ED] = {
    var grp2: HybridGraph[VD, ED] = other match {
      case grph: HybridGraph[VD, ED] => grph
      case _ => throw new IllegalArgumentException("graphs must be of the same type")
    }

    //compute new intervals
    val newIntvs: Seq[Interval] = intervalUnion(intervals, grp2.intervals)
    val newIntvsb = ProgramContext.sc.broadcast(newIntvs)

    if (span.intersects(grp2.span)) {
      val intvMap: Map[Int, Seq[Int]] = intervals.zipWithIndex.map(ii => (ii._2, newIntvs.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None))).toMap
      val intvMapB = ProgramContext.sc.broadcast(intvMap)
      val intvMap2: Map[Int, Seq[Int]] = grp2.intervals.zipWithIndex.map(ii => (ii._2, newIntvs.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None))).toMap
      val intvMap2B = ProgramContext.sc.broadcast(intvMap2)

      //for each index in a bitset, put the new one
      val gp1: ParSeq[Graph[BitSet,BitSet]] = graphs.map{g => g.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMapB.value(ii))
      }.mapEdges{ e =>
        BitSet() ++ e.attr.toSeq.flatMap(ii => intvMapB.value(ii))
      }}
      val gp2: ParSeq[Graph[BitSet,BitSet]] = grp2.graphs.map{g => g.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMap2B.value(ii))
      }.mapEdges{ e =>
        BitSet() ++ e.attr.toSeq.flatMap(ii => intvMap2B.value(ii))
      }}

      val gr1IndexStart:Int = newIntvs.indexWhere(ii => intervals.head.intersects(ii))
      val gr2IndexStart:Int = newIntvs.indexWhere(ii => grp2.intervals.head.intersects(ii))
      val gr1Pad:Int = newIntvs.size - newIntvs.lastIndexWhere(ii => intervals.last.intersects(ii)) - 1
      val gr2Pad:Int = newIntvs.size - newIntvs.lastIndexWhere(ii => grp2.intervals.last.intersects(ii)) - 1

      val grseq1:ParSeq[Graph[BitSet,BitSet]] = ParSeq.fill(gr1IndexStart){Graph[BitSet,BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)} ++ gp1 ++ ParSeq.fill(gr1Pad){Graph[BitSet,BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)}
      val grseq2:ParSeq[Graph[BitSet,BitSet]] = ParSeq.fill(gr2IndexStart){Graph[BitSet,BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)} ++ gp2 ++ ParSeq.fill(gr2Pad){Graph[BitSet,BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)}

      val gr1Sums: Seq[Int] = (Seq.fill(gr1IndexStart){1} ++ (0 +: widths).sliding(2).map{x => (x.head to x.last-1).toList.map(y => intvMap(y).size).reduce(_+_)} ++ Seq.fill(gr1Pad){1}).scanLeft(0)(_ + _).tail
      val gr2Sums: Seq[Int] = (Seq.fill(gr2IndexStart){1} ++ (0 +: grp2.widths).sliding(2).map{x => (x.head to x.last-1).toList.map(y => intvMap2(y).size).reduce(_ + _)} ++ Seq.fill(gr2Pad){1}).scanLeft(0)(_+_).tail

      var gr1Index:Int = 0
      var gr2Index:Int = 0
      var runs: Seq[Int] = Seq[Int]()
      var gps: ParSeq[Graph[BitSet,BitSet]] = ParSeq[Graph[BitSet,BitSet]]()
      //there is no union of two graphs in graphx
      var gr1VRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
      var gr1ERDD: RDD[Edge[BitSet]] = ProgramContext.sc.emptyRDD
      var gr2VRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
      var gr2ERDD: RDD[Edge[BitSet]] = ProgramContext.sc.emptyRDD
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
            (gr1ERDD union gr2ERDD).map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey(_ union _).map{ case (k,v) => Edge(k._1, k._2, v)}, BitSet(), storageLevel, storageLevel)
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
      val tmp: ED = new Array[ED](1)(0)
      val vs = gps.map(g => g.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), defaultValue)))}).reduce(_ union _)
      val es = gps.map(g => g.edges.flatMap{ case e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp)))}).reduce(_ union _)

      new HybridGraph(newIntvs, vs, es, runs, gps, defaultValue, storageLevel, false)

    } else {
      //like above, but no intervals are split, so reindexing is simpler
      //compute the starting index for each graph (with no overlap there aren't any multiples)
      val gr1IndexStart: Int = newIntvs.indexWhere(ii => intervals.head.intersects(ii))
      val gr2IndexStart: Int = newIntvs.indexWhere(ii => grp2.intervals.head.intersects(ii))

      val gp1 = if (gr1IndexStart > 0) graphs.map(g => g.mapVertices{ (vid, attr) =>
        attr.map(ii => ii + gr1IndexStart)
      }.mapEdges{ e =>
        e.attr.map(ii => ii + gr1IndexStart)
      }) else graphs
      val gp2 = if (gr2IndexStart > 0) grp2.graphs.map(g => g.mapVertices{ (vid, attr) =>
        attr.map(ii => ii + gr2IndexStart)
      }.mapEdges{ e =>
        e.attr.map(ii => ii + gr2IndexStart)
      }) else grp2.graphs

      //because there is no intersection, we can just put the sequences together
      val gps: ParSeq[Graph[BitSet,BitSet]] = if (gr1IndexStart < gr2IndexStart) gp1 ++ gp2 else gp2 ++ gp1
      val runs: Seq[Int] = if (span.end == grp2.span.start || span.start == grp2.span.end) {
        if (gr1IndexStart < gr2IndexStart) widths ++ grp2.widths else grp2.widths ++ widths
      } else {
        if (gr1IndexStart < gr2IndexStart) widths ++ Seq(grp2.widths.head +1) ++ grp2.widths.tail else grp2.widths ++ Seq(widths.head +1) ++ widths.tail
      }

      //collect vertices and edges
      val tmp: ED = new Array[ED](1)(0)
      val vs = gps.map(g => g.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), defaultValue)))}).reduce(_ union _)
      val es = gps.map(g => g.edges.flatMap{ case e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp)))}).reduce(_ union _)

      new HybridGraph(newIntvs, vs, es, runs, gps, defaultValue, storageLevel, false)

    }
  }

  override def intersection(other: TGraph[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): HybridGraph[VD, ED] = {
    defaultValue match {
      case null => intersectionStructureOnly(other)
      case _ => super.intersection(other, vFunc, eFunc).asInstanceOf[HybridGraph[VD,ED]]
    }
  }

  private def intersectionStructureOnly(other: TGraph[VD, ED]): HybridGraph[VD, ED] = {
    var grp2: HybridGraph[VD, ED] = other match {
      case grph: HybridGraph[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (span.intersects(grp2.span)) {
      //compute new intervals
      val newIntvs: Seq[Interval] = intervalIntersect(intervals, grp2.intervals)
      val newIntvsb = ProgramContext.sc.broadcast(newIntvs)
      val intvMap: Map[Int, Seq[Int]] = intervals.zipWithIndex.map(ii => (ii._2, newIntvs.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None))).toMap
      val intvMapB = ProgramContext.sc.broadcast(intvMap)
      val intvMap2: Map[Int, Seq[Int]] = grp2.intervals.zipWithIndex.map(ii => (ii._2, newIntvs.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None))).toMap
      val intvMap2B = ProgramContext.sc.broadcast(intvMap2)
      //for each index in a bitset, put the new one
      val grseq1: ParSeq[Graph[BitSet,BitSet]] = graphs.map(g => g.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMapB.value(ii))
      }.subgraph(vpred = (vid, attr) => !attr.isEmpty)
        .mapEdges( e =>
        BitSet() ++ e.attr.toSeq.flatMap(ii => intvMapB.value(ii))
      )
      ).filter(g => !g.vertices.isEmpty)
      val grseq2: ParSeq[Graph[BitSet,BitSet]] = grp2.graphs.map(g => g.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMap2B.value(ii))
      }.subgraph(vpred = (vid, attr) => !attr.isEmpty)
        .mapEdges( e =>
        BitSet() ++ e.attr.toSeq.flatMap(ii => intvMap2B.value(ii))
      )
      ).filter(g => !g.vertices.isEmpty)

      //compute new widths
      val gr1Sums: Seq[Int] = (0 +: widths).sliding(2).map{x => (x.head to x.last-1).toList.map(y => intvMap(y).size).reduce(_+_)}.dropWhile(_ == 0).toList.takeWhile(_ != 0).scanLeft(0)(_ + _).tail
      val gr2Sums: Seq[Int] = (0 +: grp2.widths).sliding(2).map{x => (x.head to x.last-1).toList.map(y => intvMap2(y).size).reduce(_+_)}.dropWhile(_ == 0).toList.takeWhile(_ != 0).scanLeft(0)(_+_).tail

      var gr1Index:Int = 0
      var gr2Index:Int = 0
      var runs: Seq[Int] = Seq[Int]()
      var gps: ParSeq[Graph[BitSet,BitSet]] = ParSeq[Graph[BitSet,BitSet]]()
      //there is no intersection of two graphs in graphx
      var gr1VRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
      var gr1ERDD: RDD[((VertexId, VertexId), BitSet)] = ProgramContext.sc.emptyRDD
      var gr2VRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
      var gr2ERDD: RDD[((VertexId, VertexId), BitSet)] = ProgramContext.sc.emptyRDD
      //how many we have accumulated so far
      var gCount:Int = 0
      var num1agg:Int = 0
      var num2agg:Int = 0

      while (gr1Index < gr1Sums.size && gr2Index < gr2Sums.size) {
        //if aligned, union
        if (gr1Sums(gr1Index) == gr2Sums(gr2Index)) {
          gr1VRDD = gr1VRDD.union(grseq1(gr1Index).vertices)
          gr2VRDD = gr2VRDD.union(grseq2(gr2Index).vertices)
          gr1ERDD = gr1ERDD.union(grseq1(gr1Index).edges.map(e => ((e.srcId, e.dstId), e.attr)))
          gr2ERDD = gr2ERDD.union(grseq2(gr2Index).edges.map(e => ((e.srcId, e.dstId), e.attr)))
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
            (gr1ERDD join gr2ERDD).mapValues{ case (a,b) => a & b}.map{ case (k,v) => Edge(k._1, k._2, v)}, BitSet(), storageLevel, storageLevel)
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
          gr1ERDD = gr1ERDD.union(grseq1(gr1Index).edges.map(e => ((e.srcId, e.dstId), e.attr)))
          gr1Index = gr1Index+1
          num1agg = num1agg + 1
        } else { //gr2Sums(gr2Index) < gr1Sums(gr1Index)
          gr2VRDD = gr2VRDD.union(grseq2(gr2Index).vertices)
          gr2ERDD = gr2ERDD.union(grseq2(gr2Index).edges.map(e => ((e.srcId, e.dstId), e.attr)))
          gr2Index = gr2Index+1
          num2agg = num2agg + 1
        }
      }

      //collect vertices and edges
      val tmp: ED = new Array[ED](1)(0)
      val vs = gps.map(g => g.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), defaultValue)))}).reduce(_ union _)
      val es = gps.map(g => g.edges.flatMap{ case e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp)))}).reduce(_ union _)

      new HybridGraph(newIntvs, vs, es, runs, gps, defaultValue, storageLevel, false)

    } else {
      emptyGraph(defaultValue)
    }

  }

  override def pregel[A: ClassTag]
  (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
    activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
    sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A): HybridGraph[VD, ED] = {
    throw new UnsupportedOperationException("pregel not yet implemented")
  }

  override def degree: RDD[(VertexId, (Interval, Int))] = {
    val mergeFunc: (HashMap[TimeIndex,Int], HashMap[TimeIndex,Int]) => HashMap[TimeIndex,Int] = { case (a,b) =>
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    val degRDDs = graphs.map(g => g.aggregateMessages[HashMap[TimeIndex, Int]](
      ctx => {
        ctx.sendToSrc(HashMap[TimeIndex,Int]() ++ ctx.attr.seq.map(x => (x,1)))
        ctx.sendToDst(HashMap[TimeIndex,Int]() ++ ctx.attr.seq.map(x => (x,1)))
      },
      mergeFunc, TripletFields.None)
    )

    val intvs = ProgramContext.sc.broadcast(intervals)
    TGraphNoSchema.coalesce(degRDDs.reduce((x: RDD[(VertexId, HashMap[TimeIndex, Int])], y: RDD[(VertexId, HashMap[TimeIndex, Int])]) => x union y).flatMap{ case (vid, map) => map.toSeq.map{ case (k,v) => (vid, (intvs.value(k), v))}})
  }


  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): HybridGraph[Double, Double] = {
    val runSums = widths.scanLeft(0)(_ + _).tail

    if (!uni) {
      val prank = (grp: Graph[BitSet,BitSet], minIndex: Int, maxIndex: Int) => {
        if (grp.edges.isEmpty)
          Graph[HashMap[TimeIndex,(Double,Double)],HashMap[TimeIndex,(Double,Double)]](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
        else {
          UndirectedPageRank.runHybrid(grp, minIndex, maxIndex-1, tol, resetProb, numIter)
        }
      }
    
      val allgs:ParSeq[Graph[HashMap[TimeIndex,(Double,Double)], HashMap[TimeIndex,(Double,Double)]]] = graphs.zipWithIndex.map{ case (g,i) => prank(g, runSums.lift(i-1).getOrElse(0), runSums(i))}

      //now extract values
      val intvs = ProgramContext.sc.broadcast(intervals)
      val vattrs= allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.toSeq.map{ case (k,v) => (vid, (intvs.value(k), v._1))}}}.reduce(_ union _)
      val eattrs = allgs.map{ g => g.edges.flatMap{ e => e.attr.toSeq.map{ case (k,v) => ((e.srcId, e.dstId), (intvs.value(k), v._1))}}}.reduce(_ union _)

      new HybridGraph(intervals, vattrs, eattrs, widths, graphs, 0.0, storageLevel, false)

    } else
      throw new UnsupportedOperationException("directed version of pagerank not yet implemented")
  }

  override def connectedComponents(): HybridGraph[VertexId, ED] = {
    val runSums = widths.scanLeft(0)(_ + _).tail

    val conc = (grp: Graph[BitSet,BitSet], minIndex: Int, maxIndex: Int) => {
    if (grp.vertices.isEmpty)
        Graph[HashMap[TimeIndex,VertexId],BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      else {
        ConnectedComponentsXT.runHybrid(grp, minIndex, maxIndex-1)
      }
    }

    val allgs = graphs.zipWithIndex.map{ case (g,i) => conc(g, runSums.lift(i-1).getOrElse(0), runSums(i))}

    //now extract values
    val intvs = ProgramContext.sc.broadcast(intervals)
    val vattrs = allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.toSeq.map{ case (k,v) => (vid, (intvs.value(k), v))}}}.reduce(_ union _)

    new HybridGraph(intervals, vattrs, allEdges, widths, graphs, -1L, storageLevel, false)
  }

  override def shortestPaths(uni: Boolean, landmarks: Seq[VertexId]): HybridGraph[Map[VertexId, Int], ED] = {
    throw new UnsupportedOperationException("shortest paths not yet implemented")
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    graphs.filterNot(_.edges.isEmpty).map(_.edges.partitions.size).reduce(_ + _)
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
  
  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int): HybridGraph[VD, ED] = {
    partitionBy(pst, runs, 0)
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): HybridGraph[VD, ED] = {
    if (pst != PartitionStrategyType.None) {
      new HybridGraph(intervals, allVertices, allEdges, widths, graphs.zipWithIndex.map { case (g,index) =>
        val numParts: Int = if (parts > 0) parts else g.edges.partitions.size
        g.partitionBy(PartitionStrategies.makeStrategy(pst, index, intervals.size, runs), numParts)}, defaultValue, storageLevel, coalesced)
    } else
      this
  }

  override protected def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false): HybridGraph[V, E] = {
    HybridGraph.fromRDDs(verts, edgs, defVal, storLevel, coalesced = coal)
  }

  override protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): HybridGraph[V, E] = HybridGraph.emptyGraph(defVal)

}

object HybridGraph extends Serializable {
  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): HybridGraph[V, E] = new HybridGraph(Seq[Interval](), ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, Seq[Int](), ParSeq[Graph[BitSet,BitSet]](), defVal, coal = true)

  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, runWidth: Int = 8, coalesced: Boolean = false): HybridGraph[V, E] = {
    val cverts = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(verts) else verts
    val cedges = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(edgs) else edgs
    val coal = coalesced | ProgramContext.eagerCoalesce

    val intervals = TGraphNoSchema.computeIntervals(cverts, cedges)

    val combined = intervals.zipWithIndex.grouped(runWidth).map{ intvs =>
      (intvs.size, 
        Graph(cverts.filter(v => v._2._1.intersects(Interval(intvs.head._1.start, intvs.last._1.end))).mapValues{v =>
          BitSet() ++ intvs.filter{ case (intv, index) => intv.intersects(v._1)}.map(ii => ii._2)
        }.reduceByKey((a,b) => a union b),
          cedges.filter(e => e._2._1.intersects(Interval(intvs.head._1.start, intvs.last._1.end))).mapValues{e =>
            BitSet() ++ intvs.filter{ case (intv, index) => intv.intersects(e._1)}.map(ii => ii._2)}.reduceByKey((a,b) => a union b).map(e => Edge(e._1._1, e._1._2, e._2)), BitSet(), storLevel, storLevel)
      )}.toList

    new HybridGraph(intervals, cverts, cedges, combined.map(x => x._1), combined.map(x => x._2).par, defVal, storLevel, coal)

  }
}
