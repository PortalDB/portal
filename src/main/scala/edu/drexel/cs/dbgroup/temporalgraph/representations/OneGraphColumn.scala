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
import java.util
import java.util.Map
import java.util.HashSet
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap

class OneGraphColumn[VD: ClassTag, ED: ClassTag](intvs: RDD[Interval], verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[((VertexId, VertexId), (Interval, ED))], grs: Graph[BitSet, BitSet], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraphNoSchema[VD, ED](intvs, verts, edgs, defValue, storLevel, coal) {

  private val graphs: Graph[BitSet, BitSet] = grs

  override def materialize() = {
    allVertices.count
    allEdges.count
    graphs.numVertices
    graphs.numEdges
  }

  /** Query operations */

  override def slice(bound: Interval): OneGraphColumn[VD, ED] = {
    //TODO: find a better way to tell whether the graphs are materialized
    if (!(graphs.edges.getStorageLevel.deserialized && intervals.getStorageLevel.deserialized)) return super.slice(bound).asInstanceOf[OneGraphColumn[VD,ED]]

    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this
    if (span.intersects(bound)) {
      val startBound = maxDate(span.start, bound.start)
      val endBound = minDate(span.end, bound.end)
      val selectBound:Interval = Interval(startBound, endBound)

      //compute indices of start and stop
      val zipped = intervals.zipWithIndex.filter(intv => intv._1.intersects(selectBound))
      val selectStart:Int = zipped.min._2.toInt
      val selectStop:Int = zipped.max._2.toInt
      val newIntvs: RDD[Interval] = zipped.map(x => x._1)

      //make a bitset that represents the selected years only
      val mask:BitSet = BitSet((selectStart to (selectStop)): _*)
      //TODO: subgraph may result in a much smaller graph, but the number
      //of partitions does not change, so coalesce may be worthwhile
      val subg = graphs.subgraph(
        vpred = (vid, attr) => !(attr & mask).isEmpty,
        epred = et => !(et.attr & mask).isEmpty)

      //now need to update indices
      val resg = subg.mapVertices((vid, vattr) => vattr.filter(x => x >= selectStart && x <= selectStop).map(_ - selectStart)).mapEdges(e => e.attr.filter(x => x >= selectStart && x <= selectStop).map(_ - selectStart))

      //now need to update the vertex attribute rdd and edge attr rdd
      val vattrs = allVertices.filter{ case (k,v) => v._1.intersects(selectBound)}
                              .mapValues( v => (Interval(maxDate(v._1.start, startBound), minDate(v._1.end, endBound)), v._2))
      val eattrs = allEdges.filter{ case (k,v) => v._1.intersects(selectBound)}
                           .mapValues( v => (Interval(maxDate(v._1.start, startBound), minDate(v._1.end, endBound)), v._2))

      new OneGraphColumn[VD, ED](newIntvs, vattrs, eattrs, resg, defaultValue, storageLevel, coalesced)

    } else
      OneGraphColumn.emptyGraph[VD,ED](defaultValue)
  }

  //assumes coalesced data
  override protected def aggregateByChange(c: ChangeSpec, vgroupby: (VertexId, VD) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): OneGraphColumn[VD, ED] = {
    //if we only have the structure, we can do efficient aggregation with the graph
    //otherwise just use the parent
    //FIXME: find a better way to tell there's only structure
    defaultValue match {
      case null if (vgroupby == vgb) => aggregateByChangeStructureOnly(c, vquant, equant)
      case _ => super.aggregateByChange(c, vgroupby, vquant, equant, vAggFunc, eAggFunc).asInstanceOf[OneGraphColumn[VD,ED]]
    }
  }
 
  private def aggregateByChangeStructureOnly(c: ChangeSpec, vquant: Quantification, equant: Quantification): OneGraphColumn[VD, ED] = {
    val size: Integer = c.num
    //TODO: get rid of collect if possible
    val grp = intervals.collect.grouped(size).toList
    val countSums = ProgramContext.sc.broadcast(grp.map{ g => g.size }.scanLeft(0)(_ + _).tail)
    val newIntvs: RDD[Interval] = intervals.zipWithIndex.map(x => ((x._2 / size), x._1)).reduceByKey((a,b) => Interval(minDate(a.start, b.start), maxDate(a.end, b.end))).sortBy(c => c._1, true).map(x => x._2)

    //TODO: get rid of collects
    val newIntvsb = ProgramContext.sc.broadcast(newIntvs.collect)
    val intvs = ProgramContext.sc.broadcast(intervals.collect)

    val filtered: Graph[BitSet, BitSet] = graphs.mapVertices { (vid, attr) =>
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
      BitSet() ++ (0 to (countSums.value.size-1)).flatMap{ case (index) =>
        //make a mask for this part
        val mask = BitSet((countSums.value.lift(index-1).getOrElse(0) to (countSums.value(index)-1)): _*)
        val tt = mask & e.attr
        if (tt.isEmpty)
          None
        else if (equant.keep(tt.toList.map(ii => intvs.value(ii).ratio(newIntvsb.value(index))).reduce(_ + _)))
          Some(index)
        else
          None
      }}
      .mapTriplets(ept => ept.attr & ept.srcAttr & ept.dstAttr)
      .subgraph(epred = et => !et.attr.isEmpty)

    val tmp: ED = new Array[ED](1)(0)
    val vs: RDD[(VertexId, (Interval, VD))] = filtered.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), defaultValue)))}
    val es: RDD[((VertexId, VertexId), (Interval, ED))] = filtered.edges.flatMap(e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp))))

    if (ProgramContext.eagerCoalesce)
      fromRDDs(vs, es, defaultValue, storageLevel, false)
    else
      new OneGraphColumn(newIntvs, vs, es, filtered, defaultValue, storageLevel, false)
  }

  override def project[ED2: ClassTag, VD2: ClassTag](emap: Edge[ED] => ED2, vmap: (VertexId, VD) => VD2, defVal: VD2): OneGraphColumn[VD2, ED2] = {
    new OneGraphColumn(intervals, allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, vmap(vid, attr)))}, allEdges.map{ case (ids, (intv, attr)) => (ids, (intv, emap(Edge(ids._1, ids._2, attr))))}, graphs, defVal, storageLevel, false)
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2, defVal: VD2)(implicit eq: VD =:= VD2 = null): OneGraphColumn[VD2, ED] = {
    new OneGraphColumn(intervals, allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, map(vid, intv, attr)))}, allEdges, graphs, defaultValue, storageLevel, false)
  }

  override def mapEdges[ED2: ClassTag](map: (Interval, Edge[ED]) => ED2): OneGraphColumn[VD, ED2] = {
    new OneGraphColumn(intervals, allVertices, allEdges.map{ case (ids, (intv, attr)) => (ids, (intv, map(intv, Edge(ids._1, ids._2, attr))))}, graphs, defaultValue, storageLevel, false)
  }

  override def union(other: TGraph[VD, ED]): OneGraphColumn[Set[VD],Set[ED]] = {
    defaultValue match {
      case null => unionStructureOnly(other)
      case _ => super.union(other).asInstanceOf[OneGraphColumn[Set[VD],Set[ED]]]
    }
  }

  private def unionStructureOnly(other: TGraph[VD, ED]): OneGraphColumn[Set[VD],Set[ED]] = {
    //TODO: if the other graph is not OG, should use the parent method which doesn't care
    var grp2: OneGraphColumn[VD, ED] = other match {
      case grph: OneGraphColumn[VD, ED] => grph
      case _ => throw new IllegalArgumentException("graphs must be of the same type")
    }

    //compute new intervals
    val newIntvs: RDD[Interval] = intervalUnion(intervals, grp2.intervals)
    //TODO: make this work without collect
    val newIntvsb = ProgramContext.sc.broadcast(newIntvs.collect)
 
    if (span.intersects(grp2.span)) {
      val intvMap: Map[Int, Seq[Int]] = intervals.collect.zipWithIndex.map(ii => (ii._2, newIntvsb.value.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMapB = ProgramContext.sc.broadcast(intvMap)
      val intvMap2: Map[Int, Seq[Int]] = grp2.intervals.collect.zipWithIndex.map(ii => (ii._2, newIntvsb.value.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMap2B = ProgramContext.sc.broadcast(intvMap2)

      //for each index in a bitset, put the new one
      val gp1: Graph[BitSet,BitSet] = graphs.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMapB.value(ii))
      }.mapEdges{ e =>
        BitSet() ++ e.attr.toSeq.flatMap(ii => intvMapB.value(ii))
      }
      val gp2: Graph[BitSet,BitSet] = grp2.graphs.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMap2B.value(ii))
      }.mapEdges{ e =>
        BitSet() ++ e.attr.toSeq.flatMap(ii => intvMap2B.value(ii))
      }

      val newGraphs: Graph[BitSet,BitSet] = Graph(gp1.vertices.union(gp2.vertices).reduceByKey((a,b) => a ++ b), gp1.edges.union(gp2.edges).map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey((a,b) => a ++ b).map(e => Edge(e._1._1, e._1._2, e._2)), BitSet(), storageLevel, storageLevel)
      val newDefVal = Set[VD]()
      val vs = newGraphs.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), newDefVal)))}
      val tmp = Set[ED]()
      val es = newGraphs.edges.flatMap(e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp))))

      if (ProgramContext.eagerCoalesce)
        fromRDDs(vs, es, newDefVal, storageLevel, false)
      else
        new OneGraphColumn(newIntvs, vs, es, newGraphs, newDefVal, storageLevel, false)

    } else {
      //like above, but no intervals are split, so reindexing is simpler
      //compute the starting index for each graph (with no overlap there aren't any multiples)
      val zipped = newIntvs.zipWithIndex
      val start = intervals.min
      val start2 = grp2.intervals.min
      val gr1IndexStart: Int = zipped.filter(intv => intv._1.intersects(start)).min._2.toInt
      val gr2IndexStart: Int = zipped.filter(intv => intv._1.intersects(start2)).min._2.toInt

      val gp1 = if (gr1IndexStart > 0) graphs.mapVertices{ (vid, attr) =>
        attr.map(ii => ii + gr1IndexStart)
      }.mapEdges{ e =>
        e.attr.map(ii => ii + gr1IndexStart)
      } else graphs
      val gp2 = if (gr2IndexStart > 0) grp2.graphs.mapVertices{ (vid, attr) =>
        attr.map(ii => ii + gr2IndexStart)
      }.mapEdges{ e =>
        e.attr.map(ii => ii + gr2IndexStart)
      } else grp2.graphs

      val newGraphs: Graph[BitSet,BitSet] = Graph(gp1.vertices.union(gp2.vertices).reduceByKey((a,b) => a ++ b), gp1.edges.union(gp2.edges).map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey((a,b) => a ++ b).map(e => Edge(e._1._1, e._1._2, e._2)), BitSet(), storageLevel, storageLevel)
      val newDefVal = Set[VD]()
      val vs = newGraphs.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), newDefVal)))}
      val tmp = Set[ED]()
      val es = newGraphs.edges.flatMap(e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp))))

      //whether the result is coalesced depends on whether the two inputs are coalesced and whether their spans meet
      val col = coalesced && grp2.coalesced && span.end != grp2.span.start && span.start != grp2.span.end
      if (ProgramContext.eagerCoalesce && !col)
        fromRDDs(vs, es, newDefVal, storageLevel, false)
      else
        new OneGraphColumn(newIntvs, vs, es, newGraphs, newDefVal, storageLevel, col)

    }
  }

  override def intersection(other: TGraph[VD, ED]): OneGraphColumn[Set[VD],Set[ED]] = {
    //TODO: find a better way to do this
    defaultValue match {
      case null => intersectionStructureOnly(other)
      case _ => super.intersection(other).asInstanceOf[OneGraphColumn[Set[VD],Set[ED]]]
    }
  }

  private def intersectionStructureOnly(other: TGraph[VD, ED]): OneGraphColumn[Set[VD],Set[ED]] = {
    //TODO: if the other graph is not OG, should use the parent method which doesn't care
    var grp2: OneGraphColumn[VD, ED] = other match {
      case grph: OneGraphColumn[VD, ED] => grph
      case _ => throw new IllegalArgumentException("graphs must be of the same type")
    }

    if (span.intersects(grp2.span)) {
      //compute new intervals
      val newIntvs: RDD[Interval] = intervalIntersect(intervals, grp2.intervals)
      //TODO: make this work without collect
      val newIntvsb = ProgramContext.sc.broadcast(newIntvs.collect)
      val intvMap: Map[Int, Seq[Int]] = intervals.collect.zipWithIndex.map(ii => (ii._2, newIntvsb.value.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMapB = ProgramContext.sc.broadcast(intvMap)
      val intvMap2: Map[Int, Seq[Int]] = grp2.intervals.collect.zipWithIndex.map(ii => (ii._2, newIntvsb.value.zipWithIndex.flatMap(jj => if (ii._1.intersects(jj._1)) Some(jj._2) else None).toList)).toMap[Int, Seq[Int]]
      val intvMap2B = ProgramContext.sc.broadcast(intvMap2)

      //for each index in a bitset, put the new one
      val gp1: Graph[BitSet,BitSet] = graphs.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMapB.value(ii))
      }.subgraph(vpred = (vid, attr) => !attr.isEmpty)
        .mapEdges{ e =>
        BitSet() ++ e.attr.toSeq.flatMap(ii => intvMapB.value(ii))
      }
      val gp2: Graph[BitSet,BitSet] = grp2.graphs.mapVertices{ (vid, attr) =>
        BitSet() ++ attr.toSeq.flatMap(ii => intvMap2B.value(ii))
      }.subgraph(vpred = (vid, attr) => !attr.isEmpty)
        .mapEdges{ e =>
        BitSet() ++ e.attr.toSeq.flatMap(ii => intvMap2B.value(ii))
      }

      //TODO: an innerJoin on edges would be more efficient
      //but it requires the exact same number of partitions and partition strategy
      //see whether repartitioning and innerJoin is better
      val newGraphs = Graph(gp1.vertices.join(gp2.vertices).mapValues{ case (a,b) => a & b}.filter(v => !v._2.isEmpty), gp1.edges.map(e => ((e.srcId, e.dstId), e.attr)).join(gp2.edges.map(e => ((e.srcId, e.dstId), e.attr))).map{ case (k, v) => Edge(k._1, k._2, v._1 & v._2)}.filter(e => !e.attr.isEmpty), BitSet(), storageLevel, storageLevel)
      val newDefVal = Set[VD]()
      val vs = newGraphs.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), newDefVal)))}
      val tmp = Set[ED]()
      val es = newGraphs.edges.flatMap(e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp))))

      //intersection of two coalesced structure-only graphs is also coalesced
      new OneGraphColumn(newIntvs, vs, es, newGraphs, newDefVal, storageLevel, this.coalesced && grp2.coalesced)

    } else {
      emptyGraph(Set(defaultValue))
    }
  }

  override def pregel[A: ClassTag]
     (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
       sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): OneGraphColumn[VD, ED] = {
    //because we run for all time instances at the same time,
    //need to convert programs and messages to the map form
    val initM: Map[TimeIndex, A] = {
      var tmp = new Int2ObjectOpenHashMap[A]()

      for(i <- 0 to intervals.count.toInt) {
        tmp.put(i, initialMsg)
      }
      tmp.asInstanceOf[Map[TimeIndex, A]]
    }

    val vertexP = (id: VertexId, attr: Map[TimeIndex, VD], msg: Map[TimeIndex, A]) => {
      var vals = attr
      msg.foreach {x =>
        val (k,v) = x
        if (vals.contains(k)) {
          vals = vals.updated(k, vprog(id, vals(k), v))
        }
      }
      vals
    }
    val sendMsgC = (edge: EdgeTriplet[Map[TimeIndex, VD], Map[TimeIndex, ED]]) => {
      //This is a hack because of a bug in GraphX that
      //does not fetch edge triplet attributes otherwise
      edge.srcAttr
      edge.dstAttr

      //sendMsg takes in an EdgeTriplet[VD,ED]
      //so we have to construct those for each TimeIndex
      edge.attr.toList.flatMap{ case (k,v) =>
        val et = new EdgeTriplet[VD, ED]
        et.srcId = edge.srcId
        et.dstId = edge.dstId
        et.srcAttr = edge.srcAttr(k)
        et.dstAttr = edge.dstAttr(k)
        et.attr = v
        //this returns Iterator[(VertexId, A)], but we need
        //Iterator[(VertexId, Map[TimeIndex, A])]
        sendMsg(et).map(x => (x._1, {var tmp = new Int2ObjectOpenHashMap[A](); tmp.put(k, x._2); tmp.asInstanceOf[Map[TimeIndex,A]]}))
      }
        .iterator
    }
    val mergeMsgC = (a: Map[TimeIndex, A], b: Map[TimeIndex, A]) => {
      val tmp = b.map { case (index, vl) => index -> mergeMsg(vl, a.getOrElse(index, defValue))}
      mapAsJavaMap(a ++ tmp)
    }

    //TODO: make this work without collect
    val intvs = ProgramContext.sc.broadcast(intervals.collect)
    val split = (interval: Interval) => {
      intvs.value.zipWithIndex.flatMap{ intv =>
        if (intv._1.intersects(interval))
          Some(intv._2)
        else
          None
      }
    }

    //need to put values into vertices and edges
    val grph = Graph[Map[TimeIndex,VD], Map[TimeIndex,ED]](
      allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => (vid, {var tmp = new Int2ObjectOpenHashMap[VD](); tmp.put(ii, attr); tmp.asInstanceOf[Map[TimeIndex, VD]]}))}.reduceByKey((a, b) => a ++ b),
      allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => (ids, {var tmp = new Int2ObjectOpenHashMap[ED](); tmp.put(ii, attr); tmp.asInstanceOf[Map[TimeIndex, ED]]}))}.reduceByKey((a,b) => a ++ b).map{ case (k,v) => Edge(k._1, k._2, v)}, new Int2ObjectOpenHashMap[VD]().asInstanceOf[Map[TimeIndex,VD]],
      storageLevel, storageLevel)
      
    val newgrp: Graph[Map[TimeIndex, VD], Map[TimeIndex, ED]] = Pregel(grph, initM, maxIterations, activeDirection)(vertexP, sendMsgC, mergeMsgC)

    val newvattrs: RDD[(VertexId, (Interval, VD))] = newgrp.vertices.flatMap{ case (vid, vattr) => vattr.toSeq.map{ case (k,v) => (vid, (intvs.value(k),v))  }}
    val neweattrs = newgrp.edges.flatMap{ e => e.attr.toSeq.map{ case (k,v) => ((e.srcId, e.dstId), (intvs.value(k), v)) }}

    if (ProgramContext.eagerCoalesce)
      fromRDDs(newvattrs, neweattrs, defaultValue, storageLevel, false)
    else
      new OneGraphColumn[VD, ED](intervals, newvattrs, neweattrs, graphs, defaultValue, storageLevel, false)
  }

  override def degree: RDD[(VertexId, (Interval, Int))] = {
    val mergedFunc: (HashMap[TimeIndex,Int], HashMap[TimeIndex,Int]) => HashMap[TimeIndex,Int] = { case (a,b) =>
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    //compute degree of each vertex for each interval
    //this should produce a map from interval to degree for each vertex
    //TODO: make this work without collect
    val intvs = ProgramContext.sc.broadcast(intervals.collect)
    val res = graphs.aggregateMessages[HashMap[TimeIndex,Int]](
      ctx => {
        ctx.sendToSrc(HashMap[TimeIndex,Int]() ++ ctx.attr.seq.map(x => (x,1)))
        ctx.sendToDst(HashMap[TimeIndex,Int]() ++ ctx.attr.seq.map(x => (x,1)))
      },
      mergedFunc,
      TripletFields.EdgeOnly)
    .flatMap{ case (vid, mp) => mp.toSeq.map{ case (k,v) => (vid, (intvs.value(k), v))}}

    TGraphNoSchema.coalesce(res)

  }

  //run pagerank on each interval
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): OneGraphColumn[(VD,Double),ED] = {
    if (!uni) {
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
          ctx.sendToSrc{var tmp = new Int2IntOpenHashMap(); ctx.attr.seq.foreach(x => tmp.put(x,1)); tmp}
          ctx.sendToDst{var tmp = new Int2IntOpenHashMap(); ctx.attr.seq.foreach(x => tmp.put(x,1)); tmp}
        },
        mergeFunc, TripletFields.EdgeOnly)

      val pagerankGraph: Graph[Int2ObjectOpenHashMap[(Double, Double)], Int2ObjectOpenHashMap[(Double, Double)]] = graphs.outerJoinVertices(degrees) {
        case (vid, vdata, Some(deg)) => vdata.filter(x => !deg.contains(x)).seq.foreach(x => deg.put(x,0)); deg
        case (vid, vdata, None) => val tmp = new Int2IntOpenHashMap(); vdata.seq.foreach(x => tmp.put(x,0)); tmp
      }
        .mapTriplets{ e => val tmp = new Int2ObjectOpenHashMap[(Double, Double)](); e.attr.seq.foreach(x => tmp.put(x, (1.0 / e.srcAttr(x), 1.0 / e.dstAttr(x)) )); tmp}
        .mapVertices( (id,attr) => {var tmp = new Int2ObjectOpenHashMap[(Double, Double)](); attr.foreach(x => tmp.put(x._1, (0.0,0.0))); tmp.map(identity); tmp})
        .cache()

      val vertexProgram = (id: VertexId, attr: Int2ObjectOpenHashMap[(Double, Double)], msg: Int2DoubleOpenHashMap) => {
        var vals = attr.clone
        msg.foreach { x =>
          val (k,v) = x
          if (attr.contains(k)) {
            val (oldPR, lastDelta) = attr(k)
            val newPR = oldPR + (1.0 - resetProb) * v
            vals.update(k,(newPR,newPR-oldPR))
          }
        }
        vals
      }

      val sendMessage = (edge: EdgeTriplet[Int2ObjectOpenHashMap[(Double, Double)], Int2ObjectOpenHashMap[(Double, Double)]]) => {
        //This is a hack because of a bug in GraphX that
        //does not fetch edge triplet attributes otherwise
        edge.srcAttr
        edge.dstAttr

        edge.attr.toList.flatMap{ case (k,v) =>
          if (edge.srcAttr.apply(k)._2 > tol &&
            edge.dstAttr.apply(k)._2 > tol) {
            Iterator((edge.dstId, {var tmp = new Int2DoubleOpenHashMap(); tmp.put(k: Int, edge.srcAttr.apply(k)._2 * v._1); tmp} ),
              (edge.srcId, {var tmp = new Int2DoubleOpenHashMap(); tmp.put(k: Int, edge.dstAttr.apply(k)._2 * v._2); tmp} ))
          } else if (edge.srcAttr.apply(k)._2 > tol) {
            Some((edge.dstId, {var tmp = new Int2DoubleOpenHashMap(); tmp.put(k: Int, edge.srcAttr.apply(k)._2 * v._1); tmp}))
          } else if (edge.dstAttr.apply(k)._2 > tol) {
            Some((edge.srcId, {var tmp = new Int2DoubleOpenHashMap(); tmp.put(k: Int, edge.dstAttr.apply(k)._2 * v._2); tmp}))
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
      var i:Int = 0
      val initialMessage:Int2DoubleOpenHashMap = {
        var tmpMap = new Int2DoubleOpenHashMap()
        for(i <- 0 to intervals.count.toInt-1) {
          tmpMap.put(i, resetProb / (1.0 - resetProb))
        }
        tmpMap
      }

      val resultGraph: Graph[Map[TimeIndex,(Double,Double)], Map[TimeIndex,(Double,Double)]] = Pregel(pagerankGraph, initialMessage, numIter, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)
        .asInstanceOf[Graph[Map[TimeIndex,(Double,Double)], Map[TimeIndex,(Double,Double)]]]

      //now need to extract the values into a separate rdd again
      //TODO: make this work without collect
      val intvs = ProgramContext.sc.broadcast(intervals.collect)
      val vattrs: RDD[(VertexId, (Interval, Double))] = resultGraph.vertices.flatMap{ case (vid,vattr) => vattr.toSeq.map{ case (k,v) => (vid,(intvs.value(k), v._1))}}
      //now need to join with the previous value
      val newverts: RDD[(VertexId, (Interval, (VD, Double)))] = allVertices.leftOuterJoin(vattrs)
        .filter{ case (k, (v, u)) => u.isEmpty || v._1.intersects(u.get._1)}
        .mapValues{ case (v, u) => if (u.isEmpty) (v._1, (v._2, 0.0)) else (v._1.intersection(u.get._1).get, (v._2, u.get._2))}

      if (ProgramContext.eagerCoalesce)
        fromRDDs(newverts, allEdges, (defaultValue, 0.0), storageLevel, false)
      else
        new OneGraphColumn[(VD, Double), ED](intervals, newverts, allEdges, graphs, (defaultValue, 0.0), storageLevel, false)

    } else {
      //TODO: implement this using pregel
      throw new UnsupportedOperationException("directed version of pageRank not yet implemented")
    }
  }
  
  //run connected components on each interval
  override def connectedComponents(): OneGraphColumn[(VD, VertexId),ED] = {
    val conGraph: Graph[Int2LongOpenHashMap, BitSet]
      = graphs.mapVertices{ case (vid, bset) => val tmp = new Int2LongOpenHashMap(); bset.foreach(x => tmp.put(x,vid)); tmp}

    val vertexProgram = (id: VertexId, attr: Int2LongOpenHashMap, msg: Int2LongOpenHashMap) => {
      var vals = attr.clone()

      msg.foreach { x =>
        val (k,v) = x
        if (attr.contains(k)) {
          vals.update(k, math.min(v, attr(k)))
        }
      }
      vals
    }

    val sendMessage = (edge: EdgeTriplet[Int2LongOpenHashMap, BitSet]) => {
      //This is a hack because of a bug in GraphX that
      //does not fetch edge triplet attributes otherwise
      edge.srcAttr
      edge.dstAttr

      edge.attr.toList.flatMap{ k =>
        if (edge.srcAttr(k) < edge.dstAttr(k))
          Some((edge.dstId, {var tmp = new Int2LongOpenHashMap(); tmp.put(k, edge.srcAttr.get(k)); tmp}))
        else if (edge.srcAttr(k) > edge.dstAttr(k))
          Some((edge.srcId, {var tmp = new Int2LongOpenHashMap(); tmp.put(k, edge.dstAttr.get(k)); tmp}))
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
    val initialMessage: Int2LongOpenHashMap = {
      var tmpMap = new Int2LongOpenHashMap()
      for(i <- 0 to intervals.count.toInt-1) {
        tmpMap.put(i, Long.MaxValue)
      }
      tmpMap
    }

    val resultGraph: Graph[Map[TimeIndex, VertexId], BitSet] = Pregel(conGraph, initialMessage, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner).asInstanceOf[Graph[Map[TimeIndex, VertexId], BitSet]]

    //TODO: make this work without collect
    val intvs = ProgramContext.sc.broadcast(intervals.collect)
    val vattrs = resultGraph.vertices.flatMap{ case (vid, vattr) => vattr.toSeq.map{ case (k,v) => (vid, (intvs.value(k), v))}}
    //now need to join with the previous value
    val newverts = allVertices.leftOuterJoin(vattrs)
      .filter{ case (k, (v, u)) => u.isEmpty || v._1.intersects(u.get._1)}
      .mapValues{ case (v, u) => if (u.isEmpty) (v._1, (v._2, -1L)) else (v._1.intersection(u.get._1).get, (v._2, u.get._2))}

    if (ProgramContext.eagerCoalesce)
      fromRDDs(newverts, allEdges, (defaultValue, -1L), storageLevel, false)
    else
      new OneGraphColumn[(VD, VertexId), ED](intervals, newverts, allEdges, graphs, (defaultValue, -1L), storageLevel, false)
  }
  
  //run shortestPaths on each interval
  override def shortestPaths(uni: Boolean, landmarks: Seq[VertexId]): OneGraphColumn[(VD, Map[VertexId, Int]), ED] = {

    if (!uni) {
      //TODO: change this to a val function
      def makeMap(x: (VertexId, Int)*): Long2IntOpenHashMap = {
        //we have to make a new map instead of modifying the input
        //because that has unintended consequences
        val itr = x.iterator;
        var tmpMap = new Long2IntOpenHashMap()

        while (itr.hasNext) {
          val k = itr.next()
          tmpMap.put(k._1, k._2)
        }
        tmpMap
      }

      val incrementMap = (spmap: Long2IntOpenHashMap) => {
        //we have to make a new map instead of modifying the input
        //because that has unintended consequences
        val itr = spmap.iterator
        var tmpMap = new Long2IntOpenHashMap()

        while (itr.hasNext) {
          val(k,v) = itr.next()
          tmpMap.put(k: Long, v+1)
        }
        tmpMap
      }

      val addMaps = (spmap1: Long2IntOpenHashMap, spmap2:Long2IntOpenHashMap) => {
        //we have to make a new map instead of modifying one of the two inputs
        //because that has unintended consequences
        val itr = (spmap1.keys ++ spmap2.keys).iterator
        var tmpMap = new Long2IntOpenHashMap()

        while(itr.hasNext){
          val k = itr.next()

          tmpMap.put(k: Long, math.min(spmap1.getOrDefault(k, Int.MaxValue), spmap2.getOrDefault(k, Int.MaxValue)))
        }
        tmpMap
      }

      val spGraph: Graph[Int2ObjectOpenHashMap[Long2IntOpenHashMap], BitSet] = graphs
      // Set the vertex attributes to vertex id for each interval
        .mapVertices { (vid, attr) =>
        if (landmarks.contains(vid)) {
          val tmp = new Int2ObjectOpenHashMap[Long2IntOpenHashMap]();
          attr.foreach(x => tmp.put(x, makeMap(vid -> 0)));
          tmp
        }
        else {
          val tmp = new Int2ObjectOpenHashMap[Long2IntOpenHashMap]();
          attr.foreach(x => tmp.put(x, makeMap()));
          tmp
        }
      }

      val initialMessage: Int2ObjectOpenHashMap[Long2IntOpenHashMap] = {
        var tmpMap = new Int2ObjectOpenHashMap[Long2IntOpenHashMap]()

        for (i <- 0 to intervals.count.toInt-1) {
          tmpMap.put(i, makeMap())
        }
        tmpMap
      }

      val addMapsCombined = (a: Int2ObjectOpenHashMap[Long2IntOpenHashMap], b: Int2ObjectOpenHashMap[Long2IntOpenHashMap]) => {
        val itr = a.iterator

        while(itr.hasNext){
          val(index, mp) = itr.next()

          b.put(index, addMaps(mp, b.getOrElse(index, makeMap())))
        }
        b
      }

      val vertexProgram = (id: VertexId, attr: Int2ObjectOpenHashMap[Long2IntOpenHashMap], msg: Int2ObjectOpenHashMap[Long2IntOpenHashMap]) => {
        //need to compute new shortestPaths to landmark for each interval
        //each edge carries a message for one interval,
        //which are combined by the combiner into a hash
        //for each interval in the msg hash, update
        var vals = attr.clone
        msg.foreach { x =>
          val (k, v) = x
          if (attr.contains(k)) {
            var newMap = addMaps(attr(k), v)
            vals.update(k, newMap)
          }
        }
        vals
      }

      val sendMessage = (edge: EdgeTriplet[Int2ObjectOpenHashMap[Long2IntOpenHashMap], BitSet]) => {
        //This is a hack because of a bug in GraphX that
        //does not fetch edge triplet attributes otherwise
        edge.srcAttr
        edge.dstAttr

        //each vertex attribute is supposed to be a map of int->spmap for each index
        edge.attr.toList.flatMap{ k =>
          val srcSpMap = edge.srcAttr(k)
          val dstSpMap = edge.dstAttr(k)
          val newAttr = incrementMap(dstSpMap)
          val newAttr2 = incrementMap(srcSpMap)

          if (srcSpMap != addMaps(newAttr, srcSpMap))
            Some((edge.srcId, new Int2ObjectOpenHashMap[Long2IntOpenHashMap](Array(k), Array(newAttr))))
          else if (dstSpMap != addMaps(newAttr2, dstSpMap))
            Some((edge.dstId, new Int2ObjectOpenHashMap[Long2IntOpenHashMap](Array(k), Array(newAttr2))))
          else
            None
        }
          .iterator
      }

      val resultGraph: Graph[Map[TimeIndex, Map[VertexId, Int]], BitSet] = Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMapsCombined)
        .asInstanceOf[Graph[Map[TimeIndex, Map[VertexId, Int]], BitSet]]

      //TODO: make this work without collect
      val intvs = ProgramContext.sc.broadcast(intervals.collect)
      val vattrs: RDD[(VertexId, (Interval, Map[VertexId, Int]))] = resultGraph.vertices.flatMap { case (vid, vattr) => vattr.toSeq.map { case (k, v) => (vid, (intvs.value(k), v)) } }
      //now need to join with the previous value
      val emptym = new Long2IntOpenHashMap().asInstanceOf[Map[VertexId, Int]]
      val newverts = allVertices.leftOuterJoin(vattrs)
        .filter{ case (k, (v, u)) => u.isEmpty || v._1.intersects(u.get._1)}
        .mapValues{ case (v, u) => if (u.isEmpty) (v._1, (v._2, emptym)) else (v._1.intersection(u.get._1).get, (v._2, u.get._2))}

      if (ProgramContext.eagerCoalesce)
        fromRDDs(newverts, allEdges, (defaultValue, emptym), storageLevel, false)
      else
        new OneGraphColumn[(VD, Map[VertexId,Int]), ED](intervals, newverts, allEdges, graphs, (defaultValue, emptym), storageLevel)
    } else {
      throw new UnsupportedOperationException("directed version of shortestPath not yet implemented")
    }
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    if (graphs.edges.isEmpty)
      0
    else
      graphs.edges.getNumPartitions
  }

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): OneGraphColumn[VD, ED] = {
    super.persist(newLevel)
    graphs.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): OneGraphColumn[VD, ED] = {
    super.unpersist(blocking)
    graphs.unpersist(blocking)
    this
  }

  override def partitionBy(tgp: TGraphPartitioning): OneGraphColumn[VD, ED] = {
    var numParts = if (tgp.parts > 0) tgp.parts else graphs.edges.getNumPartitions

    if (tgp.pst != PartitionStrategyType.None) {
      //not changing the intervals
      new OneGraphColumn[VD, ED](intervals, allVertices, allEdges, graphs.partitionByExt(PartitionStrategies.makeStrategy(tgp.pst, 0, intervals.count.toInt, tgp.runs), numParts), defaultValue, storageLevel, coalesced)
    } else
      this
  }

  override protected def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false): OneGraphColumn[V, E] = {
    OneGraphColumn.fromRDDs(verts, edgs, defVal, storLevel, coal)
  }

  override protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): OneGraphColumn[V, E] = OneGraphColumn.emptyGraph(defVal)
}

object OneGraphColumn {
  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V):OneGraphColumn[V, E] = new OneGraphColumn(ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, Graph[BitSet,BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD), defVal, coal = true)

  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): OneGraphColumn[V, E] = {
    val cverts = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(verts) else verts
    val cedges = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(edgs) else edgs
    val coal = coalesced | ProgramContext.eagerCoalesce

    val intervals = TGraphNoSchema.computeIntervals(cverts, cedges)
    val zipped = intervals.zipWithIndex

    //TODO: make this a better estimate based on evolution rate
    val redFactor:Int = 2
    val graphs: Graph[BitSet, BitSet] = Graph(cverts.cartesian(zipped).filter(x => x._1._2._1.intersects(x._2._1))
          .map(x => (x._1._1, BitSet(x._2._2.toInt)))
          .reduceByKey((a,b) => a union b, math.max(4, cverts.getNumPartitions/redFactor)),
      cedges.cartesian(zipped).filter(x => x._1._2._1.intersects(x._2._1))
          .map(x => (x._1._1, BitSet(x._2._2.toInt)))
          .reduceByKey((a,b) => a union b, math.max(4, cedges.getNumPartitions/redFactor))
          .map(e => Edge(e._1._1, e._1._2, e._2)), 
      BitSet(), storLevel, storLevel)

    new OneGraphColumn(intervals, cverts, cedges, graphs, defVal, storLevel, coal)

  }

}
