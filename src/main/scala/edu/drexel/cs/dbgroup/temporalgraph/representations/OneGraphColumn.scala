//One graph, but with attributes stored separately
//Each vertex and edge has a value attribute associated with each time period
package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.JavaConversions._
import collection.JavaConverters._
import scala.collection.immutable.BitSet
import scala.collection.breakOut
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import scala.util.control._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.impl.GraphXPartitionExtension._
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd._

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps._

import java.time.LocalDate
import java.util.Map
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap

class OneGraphColumn[VD: ClassTag, ED: ClassTag](verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[TEdge[ED]], grs: Graph[BitSet, (EdgeId, BitSet)], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends VEGraph[VD, ED](verts, edgs, defValue, storLevel, coal) {

  private var graphs: Graph[BitSet, (EdgeId,BitSet)] = grs
  private lazy val collectedIntervals: Array[Interval] = intervals.collect
  protected var partitioning = TGraphPartitioning(PartitionStrategyType.None, 1, 0)

  /** Query operations */

  override def slice(bound: Interval): OneGraphColumn[VD, ED] = {
    if (graphs == null) return super.slice(bound).partitionBy(partitioning).asInstanceOf[OneGraphColumn[VD,ED]]
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this
    if (span.intersects(bound)) {
      if (graphs == null) computeGraph()
      val startBound = maxDate(span.start, bound.start)
      val endBound = minDate(span.end, bound.end)
      val selectBound:Interval = Interval(startBound, endBound)

      //compute indices of start and stop
      val zipped = collectedIntervals.zipWithIndex.filter(intv => intv._1.intersects(selectBound))
      val selectStart:Int = zipped.min._2.toInt
      val selectStop:Int = zipped.max._2.toInt

      //make a bitset that represents the selected years only
      val mask:BitSet = BitSet((selectStart to (selectStop)): _*)
      //TODO: subgraph may result in a much smaller graph, but the number
      //of partitions does not change, so coalesce may be worthwhile
      val subg = graphs.subgraph(
        vpred = (vid, attr) => !(attr & mask).isEmpty,
        epred = et => !(et.attr._2 & mask).isEmpty)

      //now need to update indices
      val resg = subg.mapVertices((vid, vattr) => vattr.filter(x => x >= selectStart && x <= selectStop).map(_ - selectStart)).mapEdges(e => (e.attr._1, e.attr._2.filter(x => x >= selectStart && x <= selectStop).map(_ - selectStart)))

      //now need to update the vertex attribute rdd and edge attr rdd
      val vattrs = allVertices.filter{ case (k,v) => v._1.intersects(selectBound)}
                              .mapValues( v => (Interval(maxDate(v._1.start, startBound), minDate(v._1.end, endBound)), v._2))
      val eattrs = allEdges.filter{ te => te.interval.intersects(selectBound)}
                           .map{ te => 
        te.interval = Interval(maxDate(te.interval.start, startBound), minDate(te.interval.end, endBound))
        te
      }

      new OneGraphColumn[VD, ED](vattrs, eattrs, resg, defaultValue, storageLevel, coalesced)

    } else
      OneGraphColumn.emptyGraph[VD,ED](defaultValue)
  }

  //assumes coalesced data
  override  protected  def aggregateByChange(c: ChangeSpec, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): OneGraphColumn[VD, ED] = {
    defaultValue match {
      case a: StructureOnlyAttr  => aggregateByChangeStructureOnly(c, vquant, equant)
      case _ => super.aggregateByChange(c, vquant, equant, vAggFunc, eAggFunc).asInstanceOf[OneGraphColumn[VD,ED]]
    }
  }
 
  private def aggregateByChangeStructureOnly(c: ChangeSpec, vquant: Quantification, equant: Quantification): OneGraphColumn[VD, ED] = {
    val size: Integer = c.num
    if (graphs == null) computeGraph()
    //TODO: get rid of collect if possible
    val grp = collectedIntervals.grouped(size).toList
    val countSums = ProgramContext.sc.broadcast(grp.map{ g => g.size }.scanLeft(0)(_ + _).tail)
    val newIntvs: RDD[Interval] = intervals.zipWithIndex.map(x => ((x._2 / size), x._1)).reduceByKey((a,b) => Interval(minDate(a.start, b.start), maxDate(a.end, b.end))).sortBy(c => c._1, true).map(x => x._2)

    //TODO: get rid of collects
    val newIntvsb = ProgramContext.sc.broadcast(newIntvs.collect)
    val intvs = ProgramContext.sc.broadcast(collectedIntervals)

    val filtered: Graph[BitSet, (EdgeId,BitSet)] = graphs.mapVertices { (vid, attr) =>
      BitSet() ++ (0 to (countSums.value.size-1)).flatMap{ case (index) =>
        //make a mask for this part
        val mask = BitSet((countSums.value.lift(index-1).getOrElse(0) to (countSums.value(index)-1)): _*)
        val tt = mask & attr
        if (tt.isEmpty)
          None
        else if (vquant.keep(tt.toList.map(ii => intvs.value(ii).ratio(newIntvsb.value(index))).reduce(_ + _)))
          Some(index)
        else
          None
      }}
      .subgraph(vpred = (vid, attr) => !attr.isEmpty)
      .mapEdges{ e =>
      (e.attr._1, BitSet() ++ (0 to (countSums.value.size-1)).flatMap{ case (index) =>
        //make a mask for this part
        val mask = BitSet((countSums.value.lift(index-1).getOrElse(0) to (countSums.value(index)-1)): _*)
        val tt = mask & e.attr._2
        if (tt.isEmpty)
          None
        else if (equant.keep(tt.toList.map(ii => intvs.value(ii).ratio(newIntvsb.value(index))).reduce(_ + _)))
          Some(index)
        else
          None
      })}
      .mapTriplets(ept => (ept.attr._1, ept.attr._2 & ept.srcAttr & ept.dstAttr))
      .subgraph(epred = et => !et.attr._2.isEmpty)

    val tmp: ED = defaultValue.asInstanceOf[ED]
    val vs: RDD[(VertexId, (Interval, VD))] = filtered.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), defaultValue)))}
    val es: RDD[TEdge[ED]] = filtered.edges.flatMap(e => e.attr._2.toSeq.map(ii => TEdge(e.attr._1, e.srcId, e.dstId, newIntvsb.value(ii), tmp)))

    if (ProgramContext.eagerCoalesce)
      fromRDDs(vs, es, defaultValue, storageLevel, false)
    else
      new OneGraphColumn(vs, es, filtered, defaultValue, storageLevel, false)
  }

  override  protected def aggregateByTime(c: TimeSpec, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): OneGraphColumn[VD, ED] = {
    //if we only have the structure, we can do efficient aggregation with the graph
    //otherwise just use the parent
    defaultValue match {
      case a: StructureOnlyAttr  => aggregateByTimeStructureOnly(c, vquant, equant)
      case _ => super.aggregateByTime(c, vquant, equant, vAggFunc, eAggFunc).asInstanceOf[OneGraphColumn[VD,ED]]
    }

  }

  private def aggregateByTimeStructureOnly(c: TimeSpec, vquant: Quantification, equant: Quantification): OneGraphColumn[VD, ED] = {
    val start = span.start
    if (graphs == null) computeGraph()

    //make a mask which is a mapping of indices to new indices
    val newIntvs = span.split(c.res, start).map(_._2).reverse
    //TODO: get rid of collect if possible
    val indexed = collectedIntervals.zipWithIndex

    //for each index have a range of old indices from smallest to largest, inclusive
    val countSums = newIntvs.map{ intv =>
      val tmp = indexed.filter(ii => ii._1.intersects(intv))
      (tmp.head._2, tmp.last._2)
    }
    val empty: Interval = Interval.empty
    val newIntvsb = ProgramContext.sc.broadcast(newIntvs)
    val intvs = ProgramContext.sc.broadcast(collectedIntervals)
    val countSumsB = ProgramContext.sc.broadcast(countSums)

    val filtered: Graph[BitSet, (EdgeId,BitSet)] = graphs.mapVertices { (vid, attr) =>
      BitSet() ++ (0 to countSumsB.value.size-1).flatMap{ case index =>
        val mask = BitSet((countSumsB.value(index)._1 to countSumsB.value(index)._2): _*)
        val tt = mask & attr
        val newintv = newIntvsb.value(index)
        if (tt.isEmpty)
          None
        else if (vquant.keep(tt.toList.map(ii => intvs.value(ii).intersection(newintv).getOrElse(empty).ratio(newintv)).reduce(_ + _)))
          Some(index)
        else
          None
      }}
      .subgraph(vpred = (vid, attr) => !attr.isEmpty)
      .mapEdges{ e =>
      (e.attr._1, BitSet() ++ (0 to countSumsB.value.size-1).flatMap{ case index =>
        val mask = BitSet((countSumsB.value(index)._1 to countSumsB.value(index)._2): _*)
        val tt = mask & e.attr._2
        val newintv = newIntvsb.value(index)
        if (tt.isEmpty)
          None
        else if (equant.keep(tt.toList.map(ii => intvs.value(ii).intersection(newintv).getOrElse(empty).ratio(newintv)).reduce(_ + _)))
          Some(index)
        else
          None
      })}
      .mapTriplets(ept => (ept.attr._1, ept.attr._2 & ept.srcAttr & ept.dstAttr))
      .subgraph(epred = et => !et.attr._2.isEmpty)

    val tmp: ED = defaultValue.asInstanceOf[ED]
    val vs: RDD[(VertexId, (Interval, VD))] = filtered.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), defaultValue)))}
    val es: RDD[TEdge[ED]] = filtered.edges.flatMap(e => e.attr._2.map(ii => TEdge(e.attr._1, e.srcId, e.dstId, newIntvsb.value(ii), tmp)))

    if (ProgramContext.eagerCoalesce)
      fromRDDs(vs, es, defaultValue, storageLevel, false)
    else
      new OneGraphColumn(vs, es, filtered, defaultValue, storageLevel, false)

  }

  override def vmap[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2, defVal: VD2)(implicit eq: VD =:= VD2 = null): OneGraphColumn[VD2, ED] = {
    val vs = allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, map(vid, intv, attr)))}
    if (ProgramContext.eagerCoalesce)
      fromRDDs(vs, allEdges, defVal, storageLevel, false)
    else
      new OneGraphColumn(vs, allEdges, graphs, defVal, storageLevel, false)
  }

  override def emap[ED2: ClassTag](map: TEdge[ED] => ED2): OneGraphColumn[VD, ED2] = {
    val es = allEdges.map{ te =>
      TEdge[ED2](te.eId, te.srcId, te.dstId, te.interval, map(te))}
    if (ProgramContext.eagerCoalesce)
      fromRDDs(allVertices, es, defaultValue, storageLevel, false)
    else
      new OneGraphColumn(allVertices, es, graphs, defaultValue, storageLevel, false)
  }

  override def union(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): OneGraphColumn[VD,ED] = {
    defaultValue match {
      case a: StructureOnlyAttr => unionStructureOnly(other, vFunc, eFunc)
      case _ => super.union(other,vFunc,eFunc).asInstanceOf[OneGraphColumn[VD,ED]]
    }
  }

  private def unionStructureOnly(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): OneGraphColumn[VD,ED] = {
    var grp2: OneGraphColumn[VD, ED] = other match {
      case grph: OneGraphColumn[VD, ED] => grph
      case _ => return super.union(other,vFunc,eFunc).asInstanceOf[OneGraphColumn[VD,ED]]
    }

    if (graphs == null) computeGraph()
    if (grp2.graphs == null) grp2.computeGraph()

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
      val gp1: Graph[BitSet,(EdgeId,BitSet)] = graphs.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMapB.value(ii))
      }.mapEdges{ e =>
        (e.attr._1, BitSet() ++ e.attr._2.toSeq.flatMap(ii => intvMapB.value(ii)))
      }
      val gp2: Graph[BitSet,(EdgeId,BitSet)] = grp2.graphs.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMap2B.value(ii))
      }.mapEdges{ e =>
        (e.attr._1, BitSet() ++ e.attr._2.toSeq.flatMap(ii => intvMap2B.value(ii)))
      }

      val newGraphs: Graph[BitSet,(EdgeId,BitSet)] = Graph(gp1.vertices.union(gp2.vertices).reduceByKey((a,b) => a ++ b), 
        gp1.edges.union(gp2.edges)
          .map(e => ((e.attr._1, e.srcId, e.dstId), e.attr._2))
          .reduceByKey((a,b) => a ++ b)
          .map(e => Edge(e._1._2, e._1._3, (e._1._1, e._2))), 
        BitSet(), storageLevel, storageLevel)
      val newDefVal = defaultValue
      val vs = newGraphs.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), newDefVal)))}
      val tmp = defaultValue.asInstanceOf[ED]
      val es: RDD[TEdge[ED]] = newGraphs.edges.flatMap(e => e.attr._2.map(ii => TEdge(e.attr._1, e.srcId, e.dstId, newIntvsb.value(ii), tmp)))

      if (ProgramContext.eagerCoalesce)
        fromRDDs(vs, es, newDefVal, storageLevel, false)
      else
        new OneGraphColumn(vs, es, newGraphs, newDefVal, storageLevel, false)

    } else {
      //like above, but no intervals are split, so reindexing is simpler
      //compute the starting index for each graph (with no overlap there aren't any multiples)
      val zipped = newIntvs.zipWithIndex
      val start = collectedIntervals.head
      val start2 = grp2.collectedIntervals.head
      val gr1IndexStart: Int = zipped.filter(intv => intv._1.intersects(start)).min._2.toInt
      val gr2IndexStart: Int = zipped.filter(intv => intv._1.intersects(start2)).min._2.toInt

      val gp1 = if (gr1IndexStart > 0) graphs.mapVertices{ (vid, attr) =>
        attr.map(ii => ii + gr1IndexStart)
      }.mapEdges{ e =>
        (e.attr._1, e.attr._2.map(ii => ii + gr1IndexStart))
      } else graphs
      val gp2 = if (gr2IndexStart > 0) grp2.graphs.mapVertices{ (vid, attr) =>
        attr.map(ii => ii + gr2IndexStart)
      }.mapEdges{ e =>
        (e.attr._1, e.attr._2.map(ii => ii + gr2IndexStart))
      } else grp2.graphs

      val newGraphs: Graph[BitSet,(EdgeId,BitSet)] = Graph(gp1.vertices.union(gp2.vertices).reduceByKey((a,b) => a ++ b),
        gp1.edges.union(gp2.edges)
          .map(e => ((e.attr._1, e.srcId, e.dstId), e.attr._2))
          .reduceByKey((a,b) => a ++ b)
          .map(e => Edge(e._1._2, e._1._3, (e._1._1,e._2))), 
        BitSet(), storageLevel, storageLevel)
      val newDefVal = defaultValue
      val vs = newGraphs.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), newDefVal)))}
      val tmp = defaultValue.asInstanceOf[ED]
      val es: RDD[TEdge[ED]] = newGraphs.edges.flatMap(e => e.attr._2.map(ii => TEdge(e.attr._1, e.srcId, e.dstId, newIntvsb.value(ii), tmp)))

      //whether the result is coalesced depends on whether the two inputs are coalesced and whether their spans meet
      val col = coalesced && grp2.coalesced && span.end != grp2.span.start && span.start != grp2.span.end
      if (ProgramContext.eagerCoalesce && !col)
        fromRDDs(vs, es, newDefVal, storageLevel, false)
      else
        new OneGraphColumn(vs, es, newGraphs, newDefVal, storageLevel, col)

    }
  }

  override def difference(other: TGraphNoSchema[VD, ED]): OneGraphColumn[VD,ED] = {
    defaultValue match {
      case a: StructureOnlyAttr => differenceStructureOnly(other)
      case _ => super.difference(other).asInstanceOf[OneGraphColumn[VD,ED]]
    }
  }

  def differenceStructureOnly(other: TGraphNoSchema[VD, ED]): OneGraphColumn[VD, ED]  = {
    val grp2: OneGraphColumn[VD, ED] = other match {
      case grph: OneGraphColumn[VD, ED] => grph
      case _ => return super.difference(other).asInstanceOf[OneGraphColumn[VD, ED]]
    }
    if (graphs == null) computeGraph()
    if (grp2.graphs == null) grp2.computeGraph()

    //compute new intervals
    val newIntvs: RDD[Interval] = intervalDifference(intervals, grp2.intervals)
    //TODO: make this work without collect
    val newIntvsb = ProgramContext.sc.broadcast(newIntvs.collect)

    if (span.intersects(grp2.span)) {
      val intvMap: Map[Int, Seq[Int]] = collectedIntervals.zipWithIndex.map(ii => (ii._2, newIntvsb.value.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMapB = ProgramContext.sc.broadcast(intvMap)
      val intvMap2: Map[Int, Seq[Int]] = grp2.collectedIntervals.zipWithIndex.map(ii => (ii._2, newIntvsb.value.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMap2B = ProgramContext.sc.broadcast(intvMap2)

      //for each index in a bitset, put the new one
      val gp1: Graph[BitSet,(EdgeId,BitSet)] = graphs.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMapB.value(ii))
      }.mapEdges{ e =>
        (e.attr._1, BitSet() ++ e.attr._2.toSeq.flatMap(ii => intvMapB.value(ii)))
      }
      val gp2: Graph[BitSet,(EdgeId,BitSet)] = grp2.graphs.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMap2B.value(ii))
      }.mapEdges{ e =>
        (e.attr._1, BitSet() ++ e.attr._2.toSeq.flatMap(ii => intvMap2B.value(ii)))
      }
      val newGraphs: Graph[BitSet,(EdgeId,BitSet)] = 
        gp1.outerJoinVertices(gp2.vertices)((vid, attr1, attr2) => attr1.diff(attr2.getOrElse(BitSet())))
          .subgraph(vpred = (vid, attr) => !attr.isEmpty) //this will remove edges where vertices went away completely, automatically
          .mapTriplets( etp => (etp.attr._1, etp.attr._2 & etp.srcAttr & etp.dstAttr))
          .subgraph(epred = et => !et.attr._2.isEmpty)

      val vs = newGraphs.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), defaultValue)))}
      val tmp = defaultValue.asInstanceOf[ED]
      val es: RDD[TEdge[ED]] = newGraphs.edges.flatMap(e => e.attr._2.toSeq.map(ii => TEdge(e.attr._1, e.srcId, e.dstId, newIntvsb.value(ii), tmp)))

      if (ProgramContext.eagerCoalesce)
        fromRDDs(vs, es, defaultValue, storageLevel, false)
      else
        new OneGraphColumn(vs,  es, newGraphs, defaultValue, storageLevel, false)

    } else {
        this
    }
  }

  override def intersection(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): OneGraphColumn[VD,ED] = {
    defaultValue match {
      case a: StructureOnlyAttr => intersectionStructureOnly(other, vFunc, eFunc)
      case _ => super.intersection(other,vFunc,eFunc).asInstanceOf[OneGraphColumn[VD,ED]]
    }
  }

  private def intersectionStructureOnly(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): OneGraphColumn[VD,ED] = {
    var grp2: OneGraphColumn[VD, ED] = other match {
      case grph: OneGraphColumn[VD, ED] => grph
      case _ => return super.intersection(other,vFunc,eFunc).asInstanceOf[OneGraphColumn[VD,ED]]
    }

    if (span.intersects(grp2.span)) {
      if (graphs == null) computeGraph()
      if (grp2.graphs == null) grp2.computeGraph()

      //compute new intervals
      val newIntvs: RDD[Interval] = intervalIntersect(intervals, grp2.intervals)
      //TODO: make this work without collect
      val newIntvsb = ProgramContext.sc.broadcast(newIntvs.collect)
      val intvMap: Map[Int, Seq[Int]] = collectedIntervals.zipWithIndex.map(ii => (ii._2, newIntvsb.value.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMapB = ProgramContext.sc.broadcast(intvMap)
      val intvMap2: Map[Int, Seq[Int]] = grp2.collectedIntervals.zipWithIndex.map(ii => (ii._2, newIntvsb.value.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMap2B = ProgramContext.sc.broadcast(intvMap2)

      //for each index in a bitset, put the new one
      val gp1: Graph[BitSet,(EdgeId,BitSet)] = graphs.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMapB.value(ii))
      }.subgraph(vpred = (vid, attr) => !attr.isEmpty)
        .mapEdges{ e =>
        (e.attr._1, BitSet() ++ e.attr._2.toSeq.flatMap(ii => intvMapB.value(ii)))
      }
      val gp2: Graph[BitSet,(EdgeId,BitSet)] = grp2.graphs.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMap2B.value(ii))
      }.subgraph(vpred = (vid, attr) => !attr.isEmpty)
        .mapEdges{ e =>
        (e.attr._1, BitSet() ++ e.attr._2.toSeq.flatMap(ii => intvMap2B.value(ii)))
      }

      //TODO: an innerJoin on edges would be more efficient
      //but it requires the exact same number of partitions and partition strategy
      //see whether repartitioning and innerJoin is better
      val newGraphs = Graph(gp1.vertices.join(gp2.vertices).mapValues{ case (a,b) => a & b}.filter(v => !v._2.isEmpty), 
        gp1.edges.map(e => ((e.attr._1, e.srcId, e.dstId), e.attr._2))
          .join(gp2.edges.map(e => ((e.attr._1, e.srcId, e.dstId), e.attr._2)))
          .map{ case (k, v) => Edge(k._2, k._3, (k._1, v._1 & v._2))}
          .filter(e => !e.attr._2.isEmpty), 
        BitSet(), storageLevel, storageLevel)
      val newDefVal = defaultValue
      val vs = newGraphs.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), newDefVal)))}
      val tmp = defaultValue.asInstanceOf[ED]
      val es: RDD[TEdge[ED]] = newGraphs.edges.flatMap(e => e.attr._2.toSeq.map(ii => TEdge(e.attr._1, e.srcId, e.dstId, newIntvsb.value(ii), tmp)))

      //intersection of two coalesced structure-only graphs is not coalesced
      if (ProgramContext.eagerCoalesce)
        fromRDDs(vs, es, newDefVal, storageLevel, false)
      else
        new OneGraphColumn(vs, es, newGraphs, newDefVal, storageLevel, false)

    } else {
      emptyGraph(defaultValue)
    }
  }

  override def pregel[A: ClassTag]
     (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
       sendMsg: EdgeTriplet[VD, (EdgeId,ED)] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): OneGraphColumn[VD, ED] = {
    //because we run for all time instances at the same time,
    //need to convert programs and messages to the map form
    val initM: Int2ObjectOpenHashMap[A] = {
      var tmp = new Int2ObjectOpenHashMap[A]()

      for(i <- 0 to intervals.count.toInt) {
        tmp.put(i, initialMsg)
      }
      tmp
    }

    val vertexP = (id: VertexId, attr: Int2ObjectOpenHashMap[VD], msg: Int2ObjectOpenHashMap[A]) => {
      var vals = attr.clone

      val iter = attr.iterator
      while (iter.hasNext) {
        val (index,v) = iter.next
        vals.update(index, vprog(id, v, msg.getOrDefault(index, defValue)))
      }
      vals
    }
    val sendMsgC = (edge: EdgeTriplet[Int2ObjectOpenHashMap[VD], (EdgeId, Int2ObjectOpenHashMap[ED])]) => {
      //sendMsg takes in an EdgeTriplet[VD,ED]
      //so we have to construct those for each TimeIndex
      edge.attr._2.toList.flatMap{ case (k,v) =>
        val et = new EdgeTriplet[VD, (EdgeId,ED)]
        et.srcId = edge.srcId
        et.dstId = edge.dstId
        et.srcAttr = edge.srcAttr(k)
        et.dstAttr = edge.dstAttr(k)
        et.attr = (edge.attr._1,v)
        //this returns Iterator[(VertexId, A)], but we need
        //Iterator[(VertexId, Map[TimeIndex, A])]
        sendMsg(et).map{x => 
          val tmp = new Int2ObjectOpenHashMap[A]()
          tmp.put(k, x._2)
          (x._1, tmp)
        }
      }
        .iterator
    }
    val mergeMsgC = (a: Int2ObjectOpenHashMap[A], b: Int2ObjectOpenHashMap[A]) => {
      val itr = a.iterator

      while(itr.hasNext){
        val (index, oldv) = itr.next()
        b.update(index, mergeMsg(oldv, b.getOrDefault(index, defValue)))
      }
      b
    }

    //TODO: make this work without collect
    val intvs = ProgramContext.sc.broadcast(collectedIntervals)
    val split = (interval: Interval) => {
      intvs.value.zipWithIndex.flatMap{ intv =>
        if (intv._1.intersects(interval))
          Some(intv._2)
        else
          None
      }
    }

    //need to put values into vertices and edges
    val grph = Graph[Int2ObjectOpenHashMap[VD], (EdgeId, Int2ObjectOpenHashMap[ED])](
      allVertices.map{ case (vid, (intv, attr)) => 
        val tmp = new Int2ObjectOpenHashMap[VD]()
        split(intv).foreach(ii => tmp.put(ii, attr))
        (vid, tmp)
      }.reduceByKey((a, b) => (a ++ b).asInstanceOf[Int2ObjectOpenHashMap[VD]]),
      allEdges.map{ te => 
        val tmp = new Int2ObjectOpenHashMap[ED]()
        split(te.interval).foreach(ii => tmp.put(ii, te.attr))
        ((te.eId, te.srcId, te.dstId), tmp)
      }.reduceByKey((a,b) => (a ++ b).asInstanceOf[Int2ObjectOpenHashMap[ED]])
        .map{ case (k,v) => Edge(k._2, k._3, (k._1,v))}, 
      new Int2ObjectOpenHashMap[VD](), storageLevel, storageLevel)
      
    val newgrp = Pregel(grph, initM, maxIterations, activeDirection)(vertexP, sendMsgC, mergeMsgC)

    val newvattrs: RDD[(VertexId, (Interval, VD))] = newgrp.vertices.flatMap{ case (vid, vattr) => vattr.toSeq.map{ case (k,v) => (vid, (intvs.value(k),v))  }}
    val neweattrs = newgrp.edges.flatMap{ e => e.attr._2.toSeq.map{ case (k,v) => 
      TEdge(e.attr._1, e.srcId, e.dstId, intvs.value(k), v)}}

    if (ProgramContext.eagerCoalesce)
      fromRDDs(newvattrs, neweattrs, defaultValue, storageLevel, false)
    else
      new OneGraphColumn[VD, ED](newvattrs, neweattrs, graphs, defaultValue, storageLevel, false)
  }

  override def degree: RDD[(VertexId, (Interval, Int))] = {
    val mergedFunc: (HashMap[TimeIndex,Int], HashMap[TimeIndex,Int]) => HashMap[TimeIndex,Int] = { case (a,b) =>
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    //compute degree of each vertex for each interval
    //this should produce a map from interval to degree for each vertex
    //TODO: make this work without collect
    if (graphs == null) computeGraph()
    val intvs = ProgramContext.sc.broadcast(collectedIntervals)
    val res = graphs.aggregateMessages[HashMap[TimeIndex,Int]](
      ctx => {
        ctx.sendToSrc(HashMap[TimeIndex,Int]() ++ ctx.attr._2.seq.map(x => (x,1)))
        ctx.sendToDst(HashMap[TimeIndex,Int]() ++ ctx.attr._2.seq.map(x => (x,1)))
      },
      mergedFunc,
      TripletFields.EdgeOnly)
    .flatMap{ case (vid, mp) => mp.toSeq.map{ case (k,v) => (vid, (intvs.value(k), v))}}

    TGraphNoSchema.coalesce(res)

  }

  //run pagerank on each interval
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): OneGraphColumn[(VD,Double),ED] = {
    if (graphs == null) computeGraph()

    val undirected = !uni

    val mergeFunc = (a:Int2IntOpenHashMap, b:Int2IntOpenHashMap) => {
      val itr = a.iterator

      while(itr.hasNext){
        val (index, count) = itr.next()
        b.update(index, (count + b.getOrDefault(index, 0)))
      }
      b
    }

    val degrees: VertexRDD[Int2IntOpenHashMap] = graphs.aggregateMessages[Int2IntOpenHashMap](
      ctx => {
        ctx.attr._2.foreach {ii =>
          ctx.sendToSrc{new Int2IntOpenHashMap(Array(ii),Array(1))}
          if (undirected) ctx.sendToDst{new Int2IntOpenHashMap(Array(ii),Array(1))} else ctx.sendToDst{new Int2IntOpenHashMap(Array(ii),Array(0))}
        }},
      mergeFunc, TripletFields.None)

    val pagerankGraph: Graph[Int2ObjectOpenHashMap[(Double, Double)], (EdgeId,Int2ObjectOpenHashMap[(Double, Double)])] = graphs.outerJoinVertices(degrees) {
      case (vid, vdata, Some(deg)) => deg
      case (vid, vdata, None) => new Int2IntOpenHashMap()
    }
      .mapTriplets{ e => (e.attr._1, new Int2ObjectOpenHashMap[(Double, Double)](e.attr._2.toArray, e.attr._2.toArray.map(x => (1.0/e.srcAttr(x), 1.0/e.dstAttr(x)))))}
      .mapVertices( (id,attr) => new Int2ObjectOpenHashMap[(Double,Double)](attr.keySet().toIntArray(), Array.fill(attr.size)((0.0,0.0)))).cache()

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
      (edge: EdgeTriplet[Int2ObjectOpenHashMap[(Double, Double)], (EdgeId,Int2ObjectOpenHashMap[(Double, Double)])]) => {
        edge.attr._2.toList.flatMap{ case (k,v) =>
          if (edge.srcAttr.apply(k)._2 > tol &&
            edge.dstAttr.apply(k)._2 > tol) {
            Iterator((edge.dstId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.srcAttr.apply(k)._2 * v._1)) ),
              (edge.srcId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.dstAttr.apply(k)._2 * v._2))))
          } else if (edge.srcAttr.apply(k)._2 > tol) {
            Some((edge.dstId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.srcAttr.apply(k)._2 * v._1))))
          } else if (edge.dstAttr.apply(k)._2 > tol) {
            Some((edge.srcId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.dstAttr.apply(k)._2 * v._2))))
          } else {
            None
          }
        }
          .iterator
      }
      else
      (edge: EdgeTriplet[Int2ObjectOpenHashMap[(Double, Double)], (EdgeId,Int2ObjectOpenHashMap[(Double, Double)])]) => {
        edge.attr._2.toList.flatMap{ case (k,v) =>
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
        b.update(index, (count + b.getOrDefault(index, 0.0)))
      }
      b
    }

    // The initial message received by all vertices in PageRank
    //has to be a map from every interval index
    val initialMessage:Int2DoubleOpenHashMap = new Int2DoubleOpenHashMap((0 until collectedIntervals.size).toArray, Array.fill(collectedIntervals.size)(resetProb / (1.0-resetProb)))

    val dir = if (undirected) EdgeDirection.Either else EdgeDirection.Out
    val resultGraph: Graph[Map[TimeIndex,(Double,Double)], (EdgeId,Map[TimeIndex,(Double,Double)])] = Pregel(pagerankGraph, initialMessage, numIter, activeDirection = dir)(vertexProgram, sendMessage, messageCombiner)
      .asInstanceOf[Graph[Map[TimeIndex,(Double,Double)], (EdgeId,Map[TimeIndex,(Double,Double)])]]

    //now need to extract the values into a separate rdd again
    //TODO: make this work without collect
    val zipped = ProgramContext.sc.broadcast(collectedIntervals.zipWithIndex)
    val newVerts: RDD[(VertexId, (Interval, (VD, Double)))] = allVertices.leftOuterJoin(resultGraph.vertices).flatMap{ 
      case (vid, (vdata, Some(prank))) =>
        zipped.value.filter(ii => ii._1.intersects(vdata._1)).map(ii => (vid, (ii._1, (vdata._2, prank.getOrDefault(ii._2, (resetProb, 0.0))._1))))
      case (vid, (vdata, None)) => Some((vid, (vdata._1, (vdata._2, resetProb))))
    }

    if (ProgramContext.eagerCoalesce)
      fromRDDs(newVerts, allEdges, (defaultValue, 0.0), storageLevel, false)
    else
      new OneGraphColumn[(VD, Double), ED](newVerts, allEdges, graphs, (defaultValue, 0.0), storageLevel, false)

  }
  
  //run connected components on each interval
  override def connectedComponents(): OneGraphColumn[(VD, VertexId),ED] = {
    if (graphs == null) computeGraph()

    val conGraph: Graph[Int2LongOpenHashMap, (EdgeId,BitSet)]
      = graphs.mapVertices{ case (vid, bset) => new Int2LongOpenHashMap()}

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

    val i: Int = 0
    //there is really no reason to send an initial message
    val initialMessage: Int2LongOpenHashMap = new Int2LongOpenHashMap()

    val resultGraph: Graph[Map[TimeIndex, VertexId], (EdgeId,BitSet)] = Pregel(conGraph, initialMessage, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner).asInstanceOf[Graph[Map[TimeIndex, VertexId], (EdgeId,BitSet)]]

    //TODO: make this work without collect
    val zipped = ProgramContext.sc.broadcast(collectedIntervals.zipWithIndex)
    //now need to join with the previous value
    val newverts = allVertices.leftOuterJoin(resultGraph.vertices).flatMap {
      case (vid, (vdata, Some(cc))) =>
        zipped.value.filter(ii => ii._1.intersects(vdata._1)).map(ii => (vid, (ii._1, (vdata._2, cc.getOrDefault(ii._2, vid)))))
      case (vid, (vdata, None)) => Some((vid, (vdata._1, (vdata._2, vid))))
    }

    if (ProgramContext.eagerCoalesce)
      fromRDDs(newverts, allEdges, (defaultValue, -1L), storageLevel, false)
    else
      new OneGraphColumn[(VD, VertexId), ED](newverts, allEdges, graphs, (defaultValue, -1L), storageLevel, false)
  }
  
  //run shortestPaths on each interval
  override def shortestPaths(uni: Boolean, landmarks: Seq[VertexId]): OneGraphColumn[(VD, Map[VertexId, Int]), ED] = {
    if (graphs == null) computeGraph()

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

    val spGraph: Graph[Int2ObjectOpenHashMap[Long2IntOpenHashMap], (EdgeId,BitSet)] = graphs
    // Set the vertex attributes to vertex id for each interval
      .mapVertices { (vid, attr) =>
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

    val resultGraph = Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMapsCombined)

    //TODO: make this work without collect
    val zipped = ProgramContext.sc.broadcast(collectedIntervals.zipWithIndex)
    //now need to join with the previous value
    val emptyMap = new Long2IntOpenHashMap()
    val newverts = allVertices.leftOuterJoin(resultGraph.vertices).flatMap {
      case (vid, (vdata, Some(sp))) =>
        zipped.value.filter(ii => ii._1.intersects(vdata._1)).map(ii => (vid, (ii._1, (vdata._2, sp.getOrDefault(ii._2, emptyMap).asInstanceOf[Map[VertexId,Int]]))))
      case (vid, (vdata, None)) => Some((vid, (vdata._1, (vdata._2, emptyMap.asInstanceOf[Map[VertexId,Int]]))))
    }

    if (ProgramContext.eagerCoalesce)
      fromRDDs(newverts, allEdges, (defaultValue, emptyMap.asInstanceOf[Map[VertexId,Int]]), storageLevel, false)
    else
      new OneGraphColumn[(VD, Map[VertexId,Int]), ED](newverts, allEdges, graphs, (defaultValue, emptyMap.asInstanceOf[Map[VertexId,Int]]), storageLevel)
  }

  override def aggregateMessages[A: ClassTag](sendMsg: TEdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A, defVal: A, tripletFields: TripletFields = TripletFields.All): OneGraphColumn[(VD, A), ED] = {
    if (graphs == null) computeGraph()
    if(tripletFields != TripletFields.None) {
      super.aggregateMessages(sendMsg,mergeMsg,defVal,tripletFields).asInstanceOf[OneGraphColumn[(VD,A), ED]]
    }
    else {
      val agg: VertexRDD[Int2ObjectOpenHashMap[A]] = graphs.aggregateMessages[Int2ObjectOpenHashMap[A]](
        ctx => {
          val edge = ctx.toEdgeTriplet
          val triplet = new TEdgeTriplet[VD, ED]
          triplet.eId = edge.attr._1
          triplet.srcId = edge.srcId
          triplet.dstId = edge.dstId
          sendMsg(triplet).foreach { x =>
            val tmp = new Int2ObjectOpenHashMap[A]()
            ctx.attr._2.foreach (y => tmp.put(y, x._2))
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
            val (index,vl) = itr.next()
            b.update(index, mergeMsg(vl, b.getOrElse(index, defVal)))
          }
          b
        }, tripletFields)

      //now put back into vertices
      //TODO: make this work without collect
      val zipped = ProgramContext.sc.broadcast(collectedIntervals.zipWithIndex)
      val newverts: RDD[(VertexId, (Interval, (VD, A)))] = allVertices.leftOuterJoin(agg).flatMap{
        case (vid, (vdata, Some(msg))) =>
          zipped.value.filter(ii => ii._1.intersects(vdata._1)).map(ii => (vid, (ii._1, (vdata._2, msg.getOrDefault(ii._2, defVal)))))
        case (vid, (vdata, None)) =>
          Some((vid, (vdata._1, (vdata._2, defVal))))
      }

      if (ProgramContext.eagerCoalesce)
        fromRDDs(newverts, allEdges, (defaultValue, defVal), storageLevel, false)
      else
        new OneGraphColumn[(VD, A), ED](newverts, allEdges, graphs, (defaultValue, defVal), storageLevel, false)
      }
    }

  /** Spark-specific */

  override def numPartitions(): Int = {
    if (graphs == null) computeGraph()
    if (graphs.edges.isEmpty)
      0
    else
      graphs.edges.getNumPartitions
  }

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): OneGraphColumn[VD, ED] = {
    super.persist(newLevel)
    if (graphs != null) graphs.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): OneGraphColumn[VD, ED] = {
    super.unpersist(blocking)
    if (graphs != null) graphs.unpersist(blocking)
    this
  }

  override def partitionBy(tgp: TGraphPartitioning): OneGraphColumn[VD, ED] = {
    if (tgp.pst != PartitionStrategyType.None) {
      partitioning = tgp
      if (graphs != null) {
        var numParts = if (tgp.parts > 0) tgp.parts else graphs.edges.getNumPartitions
        //not changing the intervals
        new OneGraphColumn[VD, ED](allVertices, allEdges, graphs.partitionByExt(PartitionStrategies.makeStrategy(tgp.pst, 0, intervals.count.toInt, tgp.runs), numParts), defaultValue, storageLevel, coalesced)
      } else this //will apply partitioning when graphs are computed later
    } else
      this
  }

  override def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[TEdge[E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false): OneGraphColumn[V, E] = {
    OneGraphColumn.fromRDDs(verts, edgs, defVal, storLevel, coal)
  }

  override protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): OneGraphColumn[V, E] = OneGraphColumn.emptyGraph(defVal)

  protected def computeGraph(): Unit = {
    //TODO: get rid of collect if possible
    val zipped = ProgramContext.sc.broadcast(collectedIntervals.toList.zipWithIndex)
    val split: (Interval => BitSet) = (interval: Interval) => {
      BitSet() ++ zipped.value.flatMap( ii => if (interval.intersects(ii._1)) Some(ii._2) else None)
    }

    //TODO: the performance strongly depends on the number of partitions
    //need a good way to compute a good number
    graphs = Graph(allVertices.mapValues{ case (intv, attr) => split(intv)}.reduceByKey((a,b) => a union b),
      allEdges.map{ e => ((e.eId, e.srcId, e.dstId), split(e.interval))}.reduceByKey((a,b) => a union b).map(e => Edge(e._1._2, e._1._3, (e._1._1,e._2))),
      BitSet(), storageLevel, storageLevel)

    if (partitioning.pst != PartitionStrategyType.None) {
      var numParts = if (partitioning.parts > 0) partitioning.parts else graphs.edges.getNumPartitions
      graphs.partitionByExt(PartitionStrategies.makeStrategy(partitioning.pst, 0, collectedIntervals.size, partitioning.runs), numParts)
    }
  }

}

object OneGraphColumn {
  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V):OneGraphColumn[V, E] = new OneGraphColumn(ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, Graph[BitSet,(EdgeId,BitSet)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD), defVal, coal = true)

  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[TEdge[E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): OneGraphColumn[V, E] = {
    val cverts = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(verts) else verts
    val cedges = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(edgs.map(_.toPaired)).map(e => TEdge(e._1,e._2)) else edgs
    val coal = coalesced | ProgramContext.eagerCoalesce

    new OneGraphColumn(cverts, cedges, null, defVal, storLevel, coal)

  }

  def fromDataFrames[V: ClassTag, E: ClassTag](verts: org.apache.spark.sql.DataFrame, edgs: org.apache.spark.sql.DataFrame, defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): OneGraphColumn[V, E] = {
    val cverts: RDD[(VertexId, (Interval, V))] = verts.rdd.map(r => (r.getLong(0), (Interval(r.getLong(1), r.getLong(2)), r.getAs[V](3))))
    val cedgs = edgs.rdd.map(r => ((r.getLong(0), r.getLong(1), r.getLong(2)), (Interval(r.getLong(3), r.getLong(4)), r.getAs[E](5))))

    val cov = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(cverts) else cverts
    val coe = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(cedgs) else cedgs
    val coal = coalesced | ProgramContext.eagerCoalesce

    new OneGraphColumn(cov, coe.map(e => TEdge(e._1, e._2)), null, defVal, storLevel, coal)
  }

}
