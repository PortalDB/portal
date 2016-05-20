//One graph, but with attributes stored separately
//Each vertex and edge has a value attribute associated with each time period
package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.immutable.BitSet
import scala.collection.breakOut
import scala.collection.mutable.LinkedHashMap
import scala.reflect.ClassTag
import scala.util.control._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import org.apache.spark.graphx.impl.GraphXPartitionExtension._

import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd._

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps._

import java.time.LocalDate

class OneGraphColumn[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[((VertexId, VertexId), (Interval, ED))], grs: Graph[BitSet, BitSet], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY) extends TGraphNoSchema[VD, ED](intvs, verts, edgs, defValue, storLevel) {

  private val graphs: Graph[BitSet, BitSet] = grs

  override def materialize() = {
    allVertices.count
    allEdges.count
    graphs.numVertices
    graphs.numEdges
  }

  /** Query operations */

  override def slice(bound: Interval): OneGraphColumn[VD, ED] = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this

    if (span.intersects(bound)) {
      val startBound = maxDate(span.start, bound.start)
      val endBound = minDate(span.end, bound.end)
      val selectBound:Interval = Interval(startBound, endBound)

      //compute indices of start and stop
      val selectStart:Int = intervals.indexWhere(intv => intv.intersects(selectBound))
      var selectStop:Int = intervals.lastIndexWhere(intv => intv.intersects(selectBound))
      if (selectStop < 0) selectStop = intervals.size
      val newIntvs: Seq[Interval] = intervals.slice(selectStart, selectStop)

      //make a bitset that represents the selected years only
      //TODO: the mask may be very large so it may be more efficient to 
      //broadcast it
      val mask:BitSet = BitSet((selectStart to (selectStop-1)): _*)
      val subg = graphs.subgraph(
        vpred = (vid, attr) => !(attr & mask).isEmpty,
        epred = et => !(et.attr & mask).isEmpty)

      //now need to update indices
      val resg = subg.mapVertices((vid, vattr) => vattr.filter(x => x >= selectStart && x < selectStop).map(_ - selectStart)).mapEdges(e => e.attr.filter(x => x >= selectStart && x < selectStop).map(_ - selectStart))

      //now need to update the vertex attribute rdd and edge attr rdd
      val vattrs = allVertices.filter{ case (k,v) => v._1.intersects(selectBound)}.mapValues( v => (Interval(maxDate(v._1.start, startBound), minDate(v._1.end, endBound)), v._2))
      val eattrs = allEdges.filter{ case (k,v) => v._1.intersects(selectBound)}.mapValues( v => (Interval(maxDate(v._1.start, startBound), minDate(v._1.end, endBound)), v._2))

      new OneGraphColumn[VD, ED](newIntvs, vattrs, eattrs, resg, defaultValue, storageLevel)

    } else
      OneGraphColumn.emptyGraph[VD,ED](defaultValue)
  }

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
    val grp = intervals.grouped(size).toList
    val countSums = ProgramContext.sc.broadcast(grp.map{ g => g.size }.scanLeft(0)(_ + _).tail)
    val newIntvs = grp.map{ grp => grp.reduce((a,b) => Interval(a.start, b.end))}
    val newIntvsb = ProgramContext.sc.broadcast(newIntvs)
    val intvs = ProgramContext.sc.broadcast(intervals)

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
    val vs: RDD[(VertexId, (Interval, VD))] = TGraphNoSchema.coalesce(filtered.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), defaultValue)))})
    val es: RDD[((VertexId, VertexId), (Interval, ED))] = TGraphNoSchema.coalesce(filtered.edges.flatMap(e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp)))))

    new OneGraphColumn(newIntvs, vs, es, filtered, defaultValue, storageLevel)
  }

  override def union(other: TGraph[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): OneGraphColumn[VD, ED] = {
    defaultValue match {
      case null => unionStructureOnly(other)
      case _ => super.union(other, vFunc, eFunc).asInstanceOf[OneGraphColumn[VD,ED]]
    }
  }

  private def unionStructureOnly(other: TGraph[VD, ED]): OneGraphColumn[VD, ED] = {
    var grp2: OneGraphColumn[VD, ED] = other match {
      case grph: OneGraphColumn[VD, ED] => grph
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
      val vs = TGraphNoSchema.coalesce(newGraphs.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), defaultValue)))})
      val tmp: ED = new Array[ED](1)(0)
      val es = TGraphNoSchema.coalesce(newGraphs.edges.flatMap(e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp)))))

      new OneGraphColumn(newIntvs, vs, es, newGraphs, defaultValue, storageLevel)

    } else {
      //like above, but no intervals are split, so reindexing is simpler
      //compute the starting index for each graph (with no overlap there aren't any multiples)
      val gr1IndexStart: Int = newIntvs.indexWhere(ii => intervals.head.intersects(ii))
      val gr2IndexStart: Int = newIntvs.indexWhere(ii => grp2.intervals.head.intersects(ii))
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
      val vs = TGraphNoSchema.coalesce(newGraphs.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), defaultValue)))})
      val tmp: ED = new Array[ED](1)(0)
      val es = TGraphNoSchema.coalesce(newGraphs.edges.flatMap(e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp)))))

      new OneGraphColumn(newIntvs, vs, es, newGraphs, defaultValue, storageLevel)

    }
  }

  override def intersection(other: TGraph[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): OneGraphColumn[VD, ED] = {
    defaultValue match {
      case null => intersectionStructureOnly(other)
      case _ => super.intersection(other, vFunc, eFunc).asInstanceOf[OneGraphColumn[VD,ED]]
    }
  }

  private def intersectionStructureOnly(other: TGraph[VD, ED]): OneGraphColumn[VD, ED] = {
    var grp2: OneGraphColumn[VD, ED] = other match {
      case grph: OneGraphColumn[VD, ED] => grph
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

      val newGraphs = Graph(gp1.vertices.join(gp2.vertices).mapValues{ case (a,b) => a & b}.filter(v => !v._2.isEmpty), gp1.edges.innerJoin(gp2.edges)((srcId, dstId, a, b) => a & b).filter(e => !e.attr.isEmpty), BitSet(), storageLevel, storageLevel)
      val vs = TGraphNoSchema.coalesce(newGraphs.vertices.flatMap{ case (vid, bst) => bst.toSeq.map(ii => (vid, (newIntvsb.value(ii), defaultValue)))})
      val tmp: ED = new Array[ED](1)(0)
      val es = TGraphNoSchema.coalesce(newGraphs.edges.flatMap(e => e.attr.toSeq.map(ii => ((e.srcId, e.dstId), (newIntvsb.value(ii), tmp)))))

      new OneGraphColumn(newIntvs, vs, es, newGraphs, defaultValue, storageLevel)

    } else {
      emptyGraph(defaultValue)
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
    val initM: Map[TimeIndex, A] = (for(i <- 0 to intervals.size) yield (i -> initialMsg))(breakOut)
    def vertexP(id: VertexId, attr: Map[TimeIndex, VD], msg: Map[TimeIndex, A]): Map[TimeIndex, VD] = {
      var vals = attr
      msg.foreach {x =>
        val (k,v) = x
        if (vals.contains(k)) {
          vals = vals.updated(k, vprog(id, vals(k), v))
        }
      }
      vals
    }
    def sendMsgC(edge: EdgeTriplet[Map[TimeIndex, VD], Map[TimeIndex, ED]]): Iterator[(VertexId, Map[TimeIndex, A])] = {
      //sendMsg takes in an EdgeTriplet[VD,ED]
      //so we have to construct those for each TimeIndex
      edge.attr.iterator.flatMap{ case (k,v) =>
        val et = new EdgeTriplet[VD, ED]
        et.srcId = edge.srcId
        et.dstId = edge.dstId
        et.srcAttr = edge.srcAttr(k)
        et.dstAttr = edge.dstAttr(k)
        et.attr = v
        //this returns Iterator[(VertexId, A)], but we need
        //Iterator[(VertexId, Map[TimeIndex, A])]
        sendMsg(et).map(x => (x._1, Map[TimeIndex, A](k -> x._2)))
      }
        .toSeq.groupBy{ case (k,v) => k}
      //we now have a Map[VertexId, Seq[(VertexId, Map[TimeIndex,A])]]
        .mapValues(v => v.map{case (k,m) => m}.reduce((a:Map[TimeIndex,A], b:Map[TimeIndex,A]) => a ++ b))
        .iterator
    }
    def mergeMsgC(a: Map[TimeIndex, A], b: Map[TimeIndex, A]): Map[TimeIndex, A] = {
      (a.keySet ++ b.keySet).map { k =>
        k -> mergeMsg(a.getOrElse(k, defValue), b.getOrElse(k, defValue))
      }.toMap
    }

    val intvs = ProgramContext.sc.broadcast(intervals)
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
      allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => (vid, Map[TimeIndex, VD](ii -> attr)))}.reduceByKey((a, b) => a ++ b),
      allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => (ids, Map[TimeIndex, ED](ii -> attr)))}.reduceByKey((a,b) => a ++ b).map{ case (k,v) => Edge(k._1, k._2, v)},
      edgeStorageLevel = storageLevel,
      vertexStorageLevel = storageLevel)
      
    val newgrp: Graph[Map[TimeIndex, VD], Map[TimeIndex, ED]] = Pregel(grph, initM, maxIterations, activeDirection)(vertexP, sendMsgC, mergeMsgC)
    //need to convert back to bitmap and vertexattrs
    //FIXME:? is it ok that we are throwing away the new edge values?
    val newattrs: RDD[(VertexId, (Interval, VD))] = TGraphNoSchema.coalesce(newgrp.vertices.flatMap{ case (vid, vattr) => vattr.toSeq.map{ case (k,v) => (vid, (intvs.value(k),v))  }})

    new OneGraphColumn[VD, ED](intervals, newattrs, allEdges, graphs, defaultValue, storageLevel)
  }

  override def degree: RDD[(VertexId, (Interval, Int))] = {
    def mergedFunc(a:LinkedHashMap[TimeIndex,Int], b:LinkedHashMap[TimeIndex,Int]): LinkedHashMap[TimeIndex,Int] = {
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    //compute degree of each vertex for each interval
    //this should produce a map from interval to degree for each vertex
    val intvs = ProgramContext.sc.broadcast(intervals)
    val res = graphs.aggregateMessages[LinkedHashMap[TimeIndex,Int]](
      ctx => {
        ctx.sendToSrc(LinkedHashMap[TimeIndex,Int]() ++ ctx.attr.seq.map(x => (x,1)))
        ctx.sendToDst(LinkedHashMap[TimeIndex,Int]() ++ ctx.attr.seq.map(x => (x,1)))
      },
      mergedFunc,
      TripletFields.EdgeOnly)
    .flatMap{ case (vid, mp) => mp.toSeq.map{ case (k,v) => (vid, (intvs.value(k), v))}}

    TGraphNoSchema.coalesce(res)

  }

  //run pagerank on each interval
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): OneGraphColumn[Double,Double] = {

    if (uni) {
      def mergeFunc(a:Map[TimeIndex,Int], b:Map[TimeIndex,Int]): Map[TimeIndex,Int] = {
        a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
      }

      val degrees: VertexRDD[Map[TimeIndex,Int]] = graphs.aggregateMessages[Map[TimeIndex, Int]](
        ctx => {
          ctx.sendToSrc(ctx.attr.seq.map(x => (x,1)).toMap)
          ctx.sendToDst(ctx.attr.seq.map(x => (x,1)).toMap)
        },
        mergeFunc, TripletFields.None)

      val pagerankGraph: Graph[Map[TimeIndex,(Double,Double)], Map[TimeIndex,(Double,Double)]] = graphs.outerJoinVertices(degrees) {
        case (vid, vdata, Some(deg)) => deg ++ vdata.filter(x => !deg.contains(x)).seq.map(x => (x,0)).toMap
        case (vid, vdata, None) => vdata.seq.map(x => (x,0)).toMap
      }
        .mapTriplets( e =>  e.attr.seq.map(x => (x, (1.0 / e.srcAttr(x), 1.0 / e.dstAttr(x)))).toMap)
        .mapVertices( (id,attr) => attr.mapValues{ x => (0.0,0.0)}.map(identity))
        .cache()

      def vertexProgram(id: VertexId, attr: Map[TimeIndex, (Double,Double)], msg: Map[TimeIndex, Double]): Map[TimeIndex, (Double,Double)] = {
        var vals = attr
        msg.foreach { x =>
          val (k,v) = x
          if (vals.contains(k)) {
            val (oldPR, lastDelta) = vals(k)
            val newPR = oldPR + (1.0 - resetProb) * msg(k)
            vals = vals.updated(k,(newPR,newPR-oldPR))
          }
        }
        vals
      }

      def sendMessage(edge: EdgeTriplet[Map[TimeIndex,(Double,Double)], Map[TimeIndex, (Double,Double)]]) = {
        //This is a hack because of a bug in GraphX that
        //does not fetch edge triplet attributes otherwise
        edge.srcAttr
        edge.dstAttr
        //need to generate an iterator of messages for each index
        //TODO: there must be a better way to do this
        edge.attr.iterator.flatMap{ case (k,v) =>
          if (edge.srcAttr.apply(k)._2 > tol &&
            edge.dstAttr.apply(k)._2 > tol) {
            Iterator((edge.dstId, Map((k -> edge.srcAttr.apply(k)._2 * v._1))), (edge.srcId, Map((k -> edge.dstAttr.apply(k)._2 * v._2))))
          } else if (edge.srcAttr.apply(k)._2 > tol) {
            Iterator((edge.dstId, Map((k -> edge.srcAttr.apply(k)._2 * v._1))))
          } else if (edge.dstAttr.apply(k)._2 > tol) {
            Iterator((edge.srcId, Map((k -> edge.dstAttr.apply(k)._2 * v._2))))
          } else {
            Iterator.empty
          }
        }
          .toSeq.groupBy{ case (k,v) => k}
        //we now have a Map[VertexId, Seq[(VertexId, Map[TimeIndex,Double])]]
          .mapValues(v => v.map{case (k,m) => m}.reduce((a,b) => a ++ b))
          .iterator
      }
      
      def messageCombiner(a: Map[TimeIndex,Double], b: Map[TimeIndex,Double]): Map[TimeIndex,Double] = {
        (a.keySet ++ b.keySet).map { i =>
          val count1Val:Double = a.getOrElse(i, 0.0)
          val count2Val:Double = b.getOrElse(i, 0.0)
          i -> (count1Val + count2Val)
        }.toMap
      }

      // The initial message received by all vertices in PageRank
      //has to be a map from every interval index
      var i:Int = 0
      val initialMessage:Map[TimeIndex,Double] = (for(i <- 0 to intervals.size) yield (i -> resetProb / (1.0 - resetProb)))(breakOut)

      val resultGraph: Graph[Map[TimeIndex,(Double,Double)], Map[TimeIndex,(Double,Double)]] = Pregel(pagerankGraph, initialMessage, numIter, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)

      //now need to extract the values into a separate rdd again
      val intvs = ProgramContext.sc.broadcast(intervals)
      val vattrs = TGraphNoSchema.coalesce(resultGraph.vertices.flatMap{ case (vid,vattr) => vattr.toSeq.map{ case (k,v) => (vid,(intvs.value(k), v._1))}})
      val eattrs = TGraphNoSchema.coalesce(resultGraph.edges.flatMap{ e => e.attr.toSeq.map{ case (k,v) => ((e.srcId, e.dstId), (intvs.value(k), v._1))}})

      new OneGraphColumn[Double, Double](intervals, vattrs, eattrs, graphs, 0.0, storageLevel)

    } else {
      //TODO: implement this using pregel
      throw new UnsupportedOperationException("directed version of pageRank not yet implemented")
    }
  }
  
  //run connected components on each interval
  override def connectedComponents(): OneGraphColumn[VertexId,ED] = {
    val conGraph: Graph[Map[TimeIndex, VertexId], BitSet] = graphs.mapVertices{ case (vid, bset) =>
      bset.map(x => (x,vid)).toMap
    }

    def vertexProgram(id: VertexId, attr: Map[TimeIndex, VertexId], msg: Map[TimeIndex, VertexId]): Map[TimeIndex, VertexId] = {
      var vals = attr
      msg.foreach { x =>
        val (k,v) = x
        if (vals.contains(k)) {
          vals = vals.updated(k, math.min(v, vals(k)))
        }
      }
      vals
    }

    def sendMessage(edge: EdgeTriplet[Map[TimeIndex, VertexId], BitSet]): Iterator[(VertexId, Map[TimeIndex, VertexId])] = {
      //This is a hack because of a bug in GraphX that
      //does not fetch edge triplet attributes otherwise
      edge.srcAttr
      edge.dstAttr
      edge.attr.iterator.flatMap{ k =>
        if (edge.srcAttr(k) < edge.dstAttr(k))
          Iterator((edge.dstId, Map(k -> edge.srcAttr(k))))
        else if (edge.srcAttr(k) > edge.dstAttr(k))
          Iterator((edge.srcId, Map(k -> edge.dstAttr(k))))
        else
          Iterator.empty
      }
        .toSeq.groupBy{ case (k,v) => k}
        .mapValues(v => v.map{ case (k,m) => m}.reduce((a,b) => a ++ b))
        .iterator
    }

    def messageCombiner(a: Map[TimeIndex, VertexId], b: Map[TimeIndex, VertexId]): Map[TimeIndex, VertexId] = {
      (a.keySet ++ b.keySet).map { i =>
        val val1: VertexId = a.getOrElse(i, Long.MaxValue)
        val val2: VertexId = b.getOrElse(i, Long.MaxValue)
        i -> math.min(val1, val2)
      }.toMap
    }

    val i: Int = 0
    val initialMessage: Map[TimeIndex, VertexId] = (for(i <- 0 to intervals.size) yield (i -> Long.MaxValue))(breakOut)

    val resultGraph: Graph[Map[TimeIndex, VertexId], BitSet] = Pregel(conGraph, initialMessage, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)

    val intvs = ProgramContext.sc.broadcast(intervals)
    val vattrs = TGraphNoSchema.coalesce(resultGraph.vertices.flatMap{ case (vid, vattr) => vattr.toSeq.map{ case (k,v) => (vid, (intvs.value(k), v))}})

    new OneGraphColumn[VertexId, ED](intervals, vattrs, allEdges, graphs, -1L, storageLevel)
  }
  
  //run shortestPaths on each interval
  override def shortestPaths(landmarks: Seq[VertexId]): OneGraphColumn[Map[VertexId, Int], ED] = {
    def makeMap(x: (VertexId, Int)*) = Map(x: _*)

    val incrementMap = (spmap: Map[VertexId, Int]) => spmap.map { case (v, d) => v -> (d + 1) }

    val addMaps = (spmap1: Map[VertexId, Int], spmap2: Map[VertexId, Int]) =>
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

    val spGraph: Graph[Map[TimeIndex, Map[VertexId, Int]], BitSet] = graphs
    // Set the vertex attributes to vertex id for each interval
      .mapVertices { (vid, attr) =>
      if (landmarks.contains(vid))
        attr.map(x => (x, makeMap(vid -> 0))).toMap
      else
        attr.map(x => (x, makeMap())).toMap
    }

    val initialMessage: Map[TimeIndex, Map[VertexId, Int]] = (for (i <- 0 to intervals.size) yield (i -> makeMap()))(breakOut)

    def addMapsCombined(a: Map[TimeIndex, Map[VertexId, Int]], b: Map[TimeIndex, Map[VertexId, Int]]): Map[TimeIndex, Map[VertexId, Int]] = {
      (a.keySet ++ b.keySet).map { k =>
        k -> addMaps(a.getOrElse(k, makeMap()), b.getOrElse(k, makeMap()))
      }.toMap
    }

    def vertexProgram(id: VertexId, attr: Map[TimeIndex, Map[VertexId, Int]], msg: Map[TimeIndex, Map[VertexId, Int]]): Map[TimeIndex, Map[VertexId, Int]] = {
      //need to compute new shortestPaths to landmark for each interval
      //each edge carries a message for one interval,
      //which are combined by the combiner into a hash
      //for each interval in the msg hash, update
      var vals = attr
      msg.foreach { x =>
        val (k, v) = x
        if (vals.contains(k)) {
          var newMap = addMaps(attr(k), msg(k))
          vals = vals.updated(k, newMap)
        }
      }
      vals
    }

    def sendMessage(edge: EdgeTriplet[Map[TimeIndex, Map[VertexId, Int]], BitSet]): Iterator[(VertexId, Map[TimeIndex, Map[VertexId, Int]])] = {
      //each vertex attribute is supposed to be a map of int->spmap for each index
      edge.attr.iterator.flatMap{ k =>
        val srcSpMap = edge.srcAttr(k)
        val dstSpMap = edge.dstAttr(k)
        val newAttr = incrementMap(dstSpMap)
        val newAttr2 = incrementMap(srcSpMap)

        if (srcSpMap != addMaps(newAttr, srcSpMap))
          Iterator((edge.srcId, Map(k -> newAttr)))
        else if (dstSpMap != addMaps(newAttr2, dstSpMap))
          Iterator((edge.dstId, Map(k -> newAttr2)))
        else
          Iterator.empty
      }
        .toSeq.groupBy{ case (k,v) => k}
        .mapValues(v => v.map{ case (k,m) => m}.reduce((a,b) => a ++ b))
        .iterator
    }

    val resultGraph: Graph[Map[TimeIndex, Map[VertexId, Int]], BitSet] = Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMapsCombined)

    val intvs = ProgramContext.sc.broadcast(intervals)
    val vattrs: RDD[(VertexId, (Interval, Map[VertexId, Int]))] = TGraphNoSchema.coalesce(resultGraph.vertices.flatMap{ case (vid, vattr) => vattr.toSeq.map{ case (k,v) => (vid, (intvs.value(k), v))}})

    new OneGraphColumn[Map[VertexId,Int], ED](intervals, vattrs, allEdges, graphs, Map[VertexId, Int](), storageLevel)

  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    if (graphs.edges.isEmpty)
      0
    else
      graphs.edges.partitions.size
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

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int): OneGraphColumn[VD, ED] = {
    partitionBy(pst, runs, graphs.edges.partitions.size)
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): OneGraphColumn[VD, ED] = {
    var numParts = if (parts > 0) parts else graphs.edges.partitions.size

    if (pst != PartitionStrategyType.None) {
      //not changing the intervals
      new OneGraphColumn[VD, ED](intervals, allVertices, allEdges, graphs.partitionByExt(PartitionStrategies.makeStrategy(pst, 0, intervals.size, runs), numParts), defaultValue, storageLevel)
    } else
      this
  }

  override protected def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY): OneGraphColumn[V, E] = {
    OneGraphColumn.fromRDDs(verts, edgs, defVal, storLevel)
  }

  override protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): OneGraphColumn[V, E] = OneGraphColumn.emptyGraph(defVal)
}

object OneGraphColumn {
  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V):OneGraphColumn[V, E] = new OneGraphColumn(Seq[Interval](), ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, Graph[BitSet,BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD), defVal)

  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY): OneGraphColumn[V, E] = {
    val intervals = TGraphNoSchema.computeIntervals(verts, edgs)
    val broadcastIntervals = ProgramContext.sc.broadcast(intervals)

    val graphs: Graph[BitSet, BitSet] = Graph(verts.mapValues{ v => 
      BitSet() ++ broadcastIntervals.value.zipWithIndex.filter(ii => v._1.intersects(ii._1)).map(ii => ii._2)
    }.reduceByKey((a,b) => a union b),
      edgs.mapValues{ e =>
        BitSet() ++ broadcastIntervals.value.zipWithIndex.filter(ii => e._1.intersects(ii._1)).map(ii => ii._2)}.reduceByKey((a,b) => a union b).map(e => Edge(e._1._1, e._1._2, e._2)), BitSet(), storLevel, storLevel)

    new OneGraphColumn(intervals, verts, edgs, graphs, defVal, storLevel)

  }

}
