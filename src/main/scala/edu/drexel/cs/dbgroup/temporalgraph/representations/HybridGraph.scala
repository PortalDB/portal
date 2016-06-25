package edu.drexel.cs.dbgroup.temporalgraph.representations

import java.util.Map
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

class HybridGraph[VD: ClassTag, ED: ClassTag](intvs: RDD[Interval], verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[((VertexId, VertexId), (Interval, ED))], runs: Seq[Int], gps: ParSeq[Graph[BitSet, BitSet]], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraphNoSchema[VD, ED](intvs, verts, edgs, defValue, storLevel, coal) with Serializable {

  var graphs: ParSeq[Graph[BitSet, BitSet]] = gps
  //this is how many consecutive intervals are in each aggregated graph
  var widths: Seq[Int] = runs
  private lazy val collectedIntervals: Array[Interval] = intervals.collect
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
    if (graphs.size < 1) return super.slice(bound).asInstanceOf[HybridGraph[VD,ED]]
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this
    
    if (span.intersects(bound)) {
      val startBound = maxDate(span.start, bound.start)
      val endBound = minDate(span.end, bound.end)
      val selectBound:Interval = Interval(startBound, endBound)
      val start = span.start

      //compute indices of start and stop
      //start and stop both inclusive
      val zipped = intervals.zipWithIndex.filter(intv => intv._1.intersects(selectBound))
      val selectStart:Int = zipped.min._2.toInt
      val selectStop:Int = zipped.max._2.toInt
      val newIntvs: RDD[Interval] = zipped.map(x => x._1)

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
      case a: StructureOnlyAttr if (vgroupby == vgb) => aggregateByChangeStructureOnly(c, vquant, equant)
      case _ => super.aggregateByChange(c, vgroupby, vquant, equant, vAggFunc, eAggFunc).asInstanceOf[HybridGraph[VD,ED]]
    }
  }
 
  private def aggregateByChangeStructureOnly(c: ChangeSpec, vquant: Quantification, equant: Quantification): HybridGraph[VD, ED] = {
    val size: Integer = c.num
    if (graphs.size < 1) computeGraphs()

    //TODO: get rid of collect if possible
    val grp = collectedIntervals.grouped(size).toList
    val countsSum = grp.map{ g => g.size }.scanLeft(0)(_ + _).tail
    val runsSum: Seq[Int] = widths.scanLeft(0)(_ + _).tail
    val newIntvs: RDD[Interval] = intervals.zipWithIndex.map(x => ((x._2 / size), x._1)).reduceByKey((a,b) => Interval(minDate(a.start, b.start), maxDate(a.end, b.end))).sortBy(c => c._1, true).map(x => x._2)
    //TODO: get rid of collects
    val newIntvsb = ProgramContext.sc.broadcast(newIntvs.collect)
    val intvs = ProgramContext.sc.broadcast(collectedIntervals)

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

  override def project[ED2: ClassTag, VD2: ClassTag](emap: Edge[ED] => ED2, vmap: (VertexId, VD) => VD2, defVal: VD2): HybridGraph[VD2, ED2] = {
    if (graphs.size > 0)
      new HybridGraph(intervals, allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, vmap(vid, attr)))}, allEdges.map{ case (ids, (intv, attr)) => (ids, (intv, emap(Edge(ids._1, ids._2, attr))))}, widths, graphs, defVal, storageLevel, false)
    else
      super.project(emap, vmap, defVal).asInstanceOf[HybridGraph[VD2,ED2]]
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2, defVal: VD2)(implicit eq: VD =:= VD2 = null): HybridGraph[VD2, ED] = {
    if (graphs.size > 0)
      new HybridGraph(intervals, allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, map(vid, intv, attr)))}, allEdges, widths, graphs, defaultValue, storageLevel, false)
    else
      super.mapVertices(map, defVal).asInstanceOf[HybridGraph[VD2,ED]]
  }

  override def mapEdges[ED2: ClassTag](map: (Interval, Edge[ED]) => ED2): HybridGraph[VD, ED2] = {
    if (graphs.size > 0)
      new HybridGraph(intervals, allVertices, allEdges.map{ case (ids, (intv, attr)) => (ids, (intv, map(intv, Edge(ids._1, ids._2, attr))))}, widths, graphs, defaultValue, storageLevel, false)
    else
      super.mapEdges(map).asInstanceOf[HybridGraph[VD,ED2]]
  }

  override def union(other: TGraph[VD, ED]): HybridGraph[Set[VD],Set[ED]] = {
    defaultValue match {
      case a: StructureOnlyAttr => unionStructureOnly(other)
      case _ => super.union(other).asInstanceOf[HybridGraph[Set[VD],Set[ED]]]
    }
  }

  private def unionStructureOnly(other: TGraph[VD, ED]): HybridGraph[Set[VD],Set[ED]] = {
    //TODO: if the other graph is not HG, should use the parent method which doesn't care
    var grp2: HybridGraph[VD, ED] = other match {
      case grph: HybridGraph[VD, ED] => grph
      case _ => throw new IllegalArgumentException("graphs must be of the same type")
    }

    if (graphs.size < 1) computeGraphs()
    if (grp2.graphs.size < 1) grp2.computeGraphs()

    //compute new intervals
    val newIntvs: RDD[Interval] = intervalUnion(intervals, grp2.intervals)
    //TODO: make this work without collect
    val newIntvsb = ProgramContext.sc.broadcast(newIntvs.collect)

    if (span.intersects(grp2.span)) {
      val intvMap: Map[Int, Seq[Int]] = collectedIntervals.zipWithIndex.map(ii => (ii._2, newIntvsb.value.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMapB = ProgramContext.sc.broadcast(intvMap)
      val intvMap2: Map[Int, Seq[Int]] = grp2.collectedIntervals.zipWithIndex.map(ii => (ii._2, newIntvsb.value.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
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

      val zipped = newIntvs.zipWithIndex
      val start = intervals.min
      val start2 = grp2.intervals.min
      val intvs1 = zipped.filter(intv => intv._1.intersects(start))
      val intvs2 = zipped.filter(intv => intv._1.intersects(start2))
      val gr1IndexStart: Int = intvs1.min._2.toInt
      val gr2IndexStart: Int = intvs2.min._2.toInt
      val gr1Pad:Int = (zipped.count - intvs1.max._2).toInt - 1
      val gr2Pad:Int = (zipped.count - intvs2.max._2).toInt - 1

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
      val newDefVal = Set[VD]()
      val tmp = Set[ED]()
      val vs = gps.map(g => g.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), newDefVal)))}).reduce(_ union _)
      val es = gps.map(g => g.edges.flatMap{ case e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp)))}).reduce(_ union _)

      if (ProgramContext.eagerCoalesce)
        fromRDDs(vs, es, newDefVal, storageLevel, false)
      else
        new HybridGraph(newIntvs, vs, es, runs, gps, newDefVal, storageLevel, false)

    } else {
      //like above, but no intervals are split, so reindexing is simpler
      //compute the starting index for each graph (with no overlap there aren't any multiples)
      val zipped = newIntvs.zipWithIndex
      val start = intervals.min
      val start2 = grp2.intervals.min
      val gr1IndexStart: Int = zipped.filter(intv => intv._1.intersects(start)).min._2.toInt
      val gr2IndexStart: Int = zipped.filter(intv => intv._1.intersects(start2)).min._2.toInt

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
      val newDefVal = Set[VD]()
      val tmp = Set[ED]()
      val vs = gps.map(g => g.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), newDefVal)))}).reduce(_ union _)
      val es = gps.map(g => g.edges.flatMap{ case e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp)))}).reduce(_ union _)

      //whether the result is coalesced depends on whether the two inputs are coalesced and whether their spans meet
      val col = coalesced && grp2.coalesced && span.end != grp2.span.start && span.start != grp2.span.end
      if (ProgramContext.eagerCoalesce && !col)
        fromRDDs(vs, es, newDefVal, storageLevel, false)
      else
        new HybridGraph(newIntvs, vs, es, runs, gps, newDefVal, storageLevel, col)

    }
  }

  override def intersection(other: TGraph[VD, ED]): HybridGraph[Set[VD], Set[ED]] = {
    defaultValue match {
      case a: StructureOnlyAttr => intersectionStructureOnly(other)
      case _ => super.intersection(other).asInstanceOf[HybridGraph[Set[VD],Set[ED]]]
    }
  }

  private def intersectionStructureOnly(other: TGraph[VD, ED]): HybridGraph[Set[VD],Set[ED]] = {
    var grp2: HybridGraph[VD, ED] = other match {
      case grph: HybridGraph[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (span.intersects(grp2.span)) {
      if (graphs.size < 1) computeGraphs()
      if (grp2.graphs.size < 1) grp2.computeGraphs()

      //compute new intervals
      val newIntvs: RDD[Interval] = intervalIntersect(intervals, grp2.intervals)
      //TODO: make this work without collect
      val newIntvsb = ProgramContext.sc.broadcast(newIntvs.collect)
      val intvMap: Map[Int, Seq[Int]] = collectedIntervals.zipWithIndex.map(ii => (ii._2, newIntvsb.value.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMapB = ProgramContext.sc.broadcast(intvMap)
      val intvMap2: Map[Int, Seq[Int]] = grp2.collectedIntervals.zipWithIndex.map(ii => (ii._2, newIntvsb.value.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
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
      val newDefVal = Set[VD]()
      val tmp = Set[ED]()
      val vs = gps.map(g => g.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), newDefVal)))}).reduce(_ union _)
      val es = gps.map(g => g.edges.flatMap{ case e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp)))}).reduce(_ union _)

      //intersection of two coalesced structure-only graphs is also coalesced
      new HybridGraph(newIntvs, vs, es, runs, gps, newDefVal, storageLevel, this.coalesced && grp2.coalesced)

    } else {
      emptyGraph(Set(defaultValue))
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
    if (!allEdges.isEmpty) {
      if (graphs.size < 1) computeGraphs()

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

      //TODO: make this work without collect
      val intvs = ProgramContext.sc.broadcast(collectedIntervals)
      TGraphNoSchema.coalesce(degRDDs.reduce((x: RDD[(VertexId, HashMap[TimeIndex, Int])], y: RDD[(VertexId, HashMap[TimeIndex, Int])]) => x union y).flatMap{ case (vid, map) => map.toSeq.map{ case (k,v) => (vid, (intvs.value(k), v))}})
    } else
      ProgramContext.sc.emptyRDD
  }


  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): HybridGraph[(VD, Double), ED] = {
    if (graphs.size < 1) computeGraphs()
    val runSums = widths.scanLeft(0)(_ + _).tail

    if (!uni) {
      val prank = (grp: Graph[BitSet,BitSet], minIndex: Int, maxIndex: Int) => {
        if (grp.edges.isEmpty)
          Graph[Map[TimeIndex,(Double,Double)],Map[TimeIndex,(Double,Double)]](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
        else {
          UndirectedPageRank.runHybrid(grp, minIndex, maxIndex-1, tol, resetProb, numIter)
        }
      }
    
      val allgs:ParSeq[Graph[Map[TimeIndex,(Double,Double)], Map[TimeIndex,(Double,Double)]]] = graphs.zipWithIndex.map{ case (g,i) => prank(g, runSums.lift(i-1).getOrElse(0), runSums(i))}

      //now extract values
      //TODO: make this work without collect
      val intvs = ProgramContext.sc.broadcast(collectedIntervals)
      val vattrs= allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.toSeq.map{ case (k,v) => (vid, (intvs.value(k), v._1))}}}.reduce(_ union _)
      //now need to join with the previous value
      val newverts = allVertices.leftOuterJoin(vattrs)
        .filter{ case (k, (v, u)) => u.isEmpty || v._1.intersects(u.get._1)}
        .mapValues{ case (v, u) => if (u.isEmpty) (v._1, (v._2, 0.0)) else (v._1.intersection(u.get._1).get, (v._2, u.get._2))}

      if (ProgramContext.eagerCoalesce)
        fromRDDs(newverts, allEdges, (defaultValue, 0.0), storageLevel, false)
      else
        new HybridGraph(intervals, newverts, allEdges, widths, graphs, (defaultValue, 0.0), storageLevel, false)

    } else
      throw new UnsupportedOperationException("directed version of pagerank not yet implemented")
  }

  override def connectedComponents(): HybridGraph[(VD, VertexId), ED] = {
    if (graphs.size < 1) computeGraphs()
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
    //TODO: make this work without collect
    val intvs = ProgramContext.sc.broadcast(collectedIntervals)
    val vattrs = allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.asInstanceOf[Map[TimeIndex, VertexId]].toSeq.map{ case (k,v) => (vid, (intvs.value(k), v))}}}.reduce(_ union _)
    //now need to join with the previous value
    val newverts = allVertices.leftOuterJoin(vattrs)
      .filter{ case (k, (v, u)) => u.isEmpty || v._1.intersects(u.get._1)}
      .mapValues{ case (v, u) => if (u.isEmpty) (v._1, (v._2, -1L)) else (v._1.intersection(u.get._1).get, (v._2, u.get._2))}

    if (ProgramContext.eagerCoalesce)
      fromRDDs(newverts, allEdges, (defaultValue, -1L), storageLevel, false)
    else
      new HybridGraph(intervals, newverts, allEdges, widths, graphs, (defaultValue, -1L), storageLevel, false)
  }

  override def shortestPaths(uni: Boolean, landmarks: Seq[VertexId]): HybridGraph[(VD, Map[VertexId, Int]), ED] = {
    throw new UnsupportedOperationException("shortest paths not yet implemented")
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    //FIXME: this seems an overkill to compute graphs just to get the number of partitions
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
        val count = intervals.count.toInt
        new HybridGraph(intervals, allVertices, allEdges, widths, graphs.zipWithIndex.map { case (g,index) =>
          val numParts: Int = if (tgp.parts > 0) tgp.parts else g.edges.getNumPartitions
          g.partitionBy(PartitionStrategies.makeStrategy(tgp.pst, index, count, tgp.runs), numParts)}, defaultValue, storageLevel, coalesced)
      } else this
    } else
      this
  }

  override protected def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false): HybridGraph[V, E] = {
    HybridGraph.fromRDDs(verts, edgs, defVal, storLevel, coalesced = coal)
  }

  override protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): HybridGraph[V, E] = HybridGraph.emptyGraph(defVal)

  protected def computeGraphs(runWidth: Int = 8): Unit = {
    val zipped = ProgramContext.sc.broadcast(collectedIntervals.toList.zipWithIndex)
    val split: (Interval => BitSet) = (interval: Interval) => {
      BitSet() ++ zipped.value.flatMap( ii => if (interval.intersects(ii._1)) Some(ii._2) else None)
    }

    val count = collectedIntervals.size
    val redFactor:Int = math.max(1, count/runWidth)
    val vertsConverted = allVertices.mapValues{ case (intv, attr) => split(intv)}.cache()
    val edgesConverted = allEdges.mapValues{ case (intv, attr) => split(intv)}.cache()

    val vreducers = math.max(2, allVertices.getNumPartitions/redFactor)
    val ereducers = math.max(2, allEdges.getNumPartitions/redFactor)
    val combined = (0 to (count-1)).grouped(runWidth).map { intvs =>
      val set = BitSet() ++ intvs
      (intvs.size,
        Graph(vertsConverted.filter(v => !(set & v._2).isEmpty)
              .reduceByKey((a,b) => a union b, vreducers),
          edgesConverted.filter(e => !(set & e._2).isEmpty)
              .reduceByKey((a,b) => a union b, ereducers)
              .map(e => Edge(e._1._1, e._1._2, e._2)),
          BitSet(), storLevel, storLevel)
      )}.toList

    widths = combined.map(x => x._1)
    graphs = combined.map(x => x._2).par

    if (partitioning.pst != PartitionStrategyType.None) {
      val count = intervals.count.toInt
      graphs = graphs.zipWithIndex.map { case (g, index) =>
        val numParts: Int = if (partitioning.parts > 0) partitioning.parts else g.edges.getNumPartitions
        g.partitionBy(PartitionStrategies.makeStrategy(partitioning.pst, index, count, partitioning.runs), numParts)}
    }
  }

}

object HybridGraph extends Serializable {
  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): HybridGraph[V, E] = new HybridGraph(ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, Seq[Int](), ParSeq[Graph[BitSet,BitSet]](), defVal, coal = true)

  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): HybridGraph[V, E] = {
    val cverts = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(verts) else verts
    val cedges = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(edgs) else edgs
    val coal = coalesced | ProgramContext.eagerCoalesce

    val intervals = TGraphNoSchema.computeIntervals(cverts, cedges)

    //because we use "lazy" evaluation, we don't compute the graphs until we need them
    new HybridGraph(intervals, cverts, cedges, Seq[Int](), ParSeq[Graph[BitSet,BitSet]](), defVal, storLevel, coal)
  }
}
