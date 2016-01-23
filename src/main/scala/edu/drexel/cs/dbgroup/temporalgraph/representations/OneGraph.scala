//One graph
//Each vertex and edge has a value attribute associated with each time period
package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.parallel.ParSeq
import scala.collection.mutable.HashMap
import scala.collection.mutable.Buffer
import scala.collection.breakOut
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
import edu.drexel.cs.dbgroup.temporalgraph.util.MultifileLoad
import java.time.LocalDate

class OneGraph[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], grs: Graph[Map[TimeIndex, VD], Map[TimeIndex, ED]]) extends TemporalGraph[VD, ED] with Serializable {
  val graphs: Graph[Map[TimeIndex, VD], Map[TimeIndex, ED]] = grs
  //because intervals are consecutive and equally sized,
  //we could store just the start of each one
  val resolution:Resolution = if (intvs.size > 0) intvs.head.resolution else Resolution.zero

  intvs.foreach { ii =>
    if (!ii.resolution.isEqual(resolution))
      throw new IllegalArgumentException("temporal sequence is not valid, intervals are not all of equal resolution")
  }

  val intervals: Seq[Interval] = intvs
  lazy val span: Interval = if (intervals.size > 0) Interval(intervals.head.start, intervals.last.end) else Interval(LocalDate.now, LocalDate.now)

  /** Default constructor is provided to support serialization */
  protected def this() = this(Seq[Interval](), Graph[Map[TimeIndex, VD], Map[TimeIndex, ED]](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD))

  override def size(): Int = intervals.size

  override def materialize() = {
    graphs.numVertices
    graphs.numEdges
  }

  override def vertices: VertexRDD[Map[Interval, VD]] = {
    val start = span.start
    graphs.vertices.mapValues(v => v.map(x => (resolution.getInterval(start, x._1) -> x._2)))
  }

  override def verticesFlat: VertexRDD[(Interval, VD)] = {
    val start = span.start
    VertexRDD(graphs.vertices.flatMap(v => v._2.map(x => (v._1, (resolution.getInterval(start, x._1), x._2)))))
  }

  override def edges: EdgeRDD[Map[Interval, ED]] = {
    val start = span.start
    graphs.edges.mapValues(e => e.attr.map(x => (resolution.getInterval(start, x._1) -> x._2)))
  }

  override def edgesFlat: EdgeRDD[(Interval, ED)] = {
    val start = span.start
    EdgeRDD.fromEdges[(Interval, ED), VD](graphs.edges.flatMap(e => e.attr.map(x => Edge(e.srcId, e.dstId, (resolution.getInterval(start, x._1), x._2)))))
  }

  override def degrees: VertexRDD[Map[Interval, Int]] = {
    def mergedFunc(a:Map[TimeIndex,Int], b:Map[TimeIndex,Int]): Map[TimeIndex,Int] = {
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    //compute degree of each vertex for each interval
    //this should produce a map from interval to degree for each vertex
    val start = span.start
    graphs.aggregateMessages[Map[TimeIndex,Int]](
      ctx => {
        ctx.sendToSrc(ctx.attr.mapValues(x => 1))
        ctx.sendToDst(ctx.attr.mapValues(x => 1))
      },
      mergedFunc,
      TripletFields.None)
    .mapValues(v => v.map(x => (resolution.getInterval(start, x._1) -> x._2)))
  }

  override def getTemporalSequence: Seq[Interval] = intervals

  override def getSnapshot(period: Interval): Graph[VD,ED] = {
    val index = intervals.indexOf(period)
    graphs.subgraph(
      vpred = (vid, attr) => !attr.filter{ case (k,v) => k == index}.isEmpty,
      epred = et => !et.attr.filter{ case (k,v) => k == index}.isEmpty)
      .mapVertices((vid,vattr) => vattr(index))
      .mapEdges(e => e.attr(index))
  }

  /** Query operations */

  override def select(bound: Interval): TemporalGraph[VD, ED] = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this

    if (span.intersects(bound)) {
      val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
      val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end
      val start = span.start

      //compute indices of start and stop
      val selectStart:Int = intervals.indexOf(resolution.getInterval(startBound))
      var selectStop:Int = intervals.indexOf(resolution.getInterval(endBound))
      if (selectStop < 0) selectStop = intervals.size

      val newIntvs: Seq[Interval] = intervals.slice(selectStart, selectStop)

      //now select the vertices and edges
      val subg = graphs.subgraph(
        vpred = (vid, attr) => !attr.filter{ case (k,v) => k >= selectStart && k < selectStop}.isEmpty,
        epred = et => !et.attr.filter{ case (k,v) => k >= selectStart && k < selectStop}.isEmpty)

      //now need to renumber vertex and edge intervals indices
      //indices outside of selected range get dropped
      val resg = subg.mapVertices((vid,vattr) => vattr.filter{case (k,v) => k >= selectStart && k < selectStop}.map(x => (x._1 - selectStart -> x._2))).mapEdges(e => e.attr.filter{case (k,v) => k >= selectStart && k < selectStop}.map(x => (x._1 - selectStart -> x._2)))

      new OneGraph[VD, ED](newIntvs, resg)

    } else
      OneGraph.emptyGraph[VD,ED]()
  }

  override def select(tpred: Interval => Boolean): TemporalGraph[VD, ED] = {
    val start = span.start
    new OneGraph[VD, ED](intervals, graphs.subgraph(
      vpred = (vid, attr) => !attr.filter{ case (k,v) => tpred(resolution.getInterval(start, k))}.isEmpty,
      epred = et => !et.attr.filter{ case (k,v) => tpred(resolution.getInterval(start, k))}.isEmpty))
  }

  override def select(epred: EdgeTriplet[VD,ED] => Boolean, vpred: (VertexId, VD) => Boolean): TemporalGraph[VD, ED] = {
    //TODO: implement this
      throw new UnsupportedOperationException("this version of select not yet implementet")
  }

  override def aggregate(res: Resolution, vsem: AggregateSemantics.Value, esem: AggregateSemantics.Value, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    var intvs: Seq[Interval] = Seq[Interval]()

    if (!resolution.isCompatible(res)) {
      throw new IllegalArgumentException("incompatible resolution")
    }

    //it is possible that different number of graphs end up in different intervals
    //such as going from days to months
    var index:Integer = 0
    //make a map of old indices to new ones
    val indMap:HashMap[TimeIndex, TimeIndex] = HashMap[TimeIndex, TimeIndex]()
    val cntMap:scala.collection.mutable.Map[TimeIndex, Int] = HashMap[TimeIndex, Int]().withDefaultValue(0)

    while (index < intervals.size) {
      val intv:Interval = intervals(index)
      //need to compute the interval start and end based on resolution new units
      val newIntv:Interval = res.getInterval(intv.start)
      val expected:Integer = resolution.getNumParts(res, intv.start)

      indMap(index) = intvs.size
      index += 1

      //grab all the intervals that fit within
      val loop = new Breaks
      loop.breakable {
        while (index < intervals.size) {
          val intv2:Interval = intervals(index)
          if (newIntv.contains(intv2)) {
            indMap(index) = intvs.size
            index += 1
          } else {
            loop.break
          }
        }
      }

      cntMap(intvs.size) = expected
      intvs = intvs :+ newIntv
    }

    //for each vertex, make a new list of indices
    //then filter out those vertices that have no indices
    val filtered = graphs.mapVertices { (vid, attr) =>
      var tmp: Map[Int, Seq[VD]] = attr.toSeq.map { case (k, v) => (indMap(k), v) }.groupBy { case (k, v) => k }.mapValues { v => v.map { case (x, y) => y } }
      //tmp is now a map of (index, list(attr))
      if (vsem == AggregateSemantics.All) {
        tmp = tmp.filter { case (k, v) => v.size == cntMap(k) }
      }
      tmp.mapValues { v => v.reduce(vAggFunc) }.map(identity)
    }
    .subgraph(vpred = (vid, attr) => !attr.isEmpty)
    .mapEdges { e =>
      var tmp: Map[Int, Seq[ED]] = e.attr.toSeq.map { case (k,v) => (indMap(k), v) }.groupBy { case (k,v) => k}.mapValues { v => v.map { case (x, y) => y } }
      //tmp is now a map of (index, list(attr))
      if (esem == AggregateSemantics.All) {
        tmp = tmp.filter { case (k,v) => v.size == cntMap(k) }
      }
      tmp.mapValues { v => v.reduce(eAggFunc) }.map(identity)
    }
    .subgraph(epred = e => !e.attr.isEmpty)

    new OneGraph[VD, ED](intvs, filtered)
  }

  override def transform[ED2: ClassTag, VD2: ClassTag](emap: (Edge[ED], Interval) => ED2, vmap: (VertexId, Interval, VD) => VD2): TemporalGraph[VD2, ED2] = {
    val start = span.start
    new OneGraph[VD2, ED2](intervals, graphs.mapVertices{ (id: VertexId, attr: Map[TimeIndex, VD]) => attr.map(x => (x._1, vmap(id, resolution.getInterval(start, x._1), x._2)))}
      .mapEdges{ e: Edge[Map[TimeIndex, ED]] => e.attr.map(x => (x._1, emap(Edge(e.srcId, e.dstId, x._2), resolution.getInterval(start, x._1))))})
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val start = span.start
    new OneGraph[VD2, ED](intervals, graphs.mapVertices{ (id: VertexId, attr: Map[TimeIndex, VD]) =>
      attr.map(x => (x._1, map(id, resolution.getInterval(start, x._1), x._2)))
    }
    )
  }

  override def mapEdges[ED2: ClassTag](map: (Edge[ED], Interval) => ED2): TemporalGraph[VD, ED2] = {
    val start = span.start
    new OneGraph[VD, ED2](intervals, graphs.mapEdges{ e: Edge[Map[TimeIndex, ED]] =>
      e.attr.map(x => (x._1, map(Edge(e.srcId, e.dstId, x._2), resolution.getInterval(start, x._1))))
    })
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, Map[Interval, U])])(mapFunc: (VertexId, Interval, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    //convert the intervals to corresponding indices
    val in:RDD[(VertexId, Map[TimeIndex, U])] = other.mapValues { attr: Map[Interval, U] =>
      attr.filterKeys(k => span.contains(k)).map{ case (intvl, attr) => (intervals.indexOf(intvl), attr)}
    }
    val start = span.start
    new OneGraph[VD2, ED](intervals, graphs.outerJoinVertices(in){ (id: VertexId, attr1: Map[TimeIndex, VD], attr2: Option[Map[TimeIndex, U]]) =>
      if (attr2.isEmpty)
        attr1.map(x => (x._1, mapFunc(id, resolution.getInterval(start, x._1), x._2, None)))
      else
        attr1.map(x => (x._1, mapFunc(id, resolution.getInterval(start, x._1), x._2, attr2.get.get(x._1))))
    })
  }

  override def union(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    /** The type checking already validates that the structurally the graphs are union-compatible
      * But we also need to check that they are temporally union-compatible
      * this includes having the same resolution and aligning intervals
      * Two temporal sequences are compatible if their first elements are compatible
      */ 
    var grp2: OneGraph[VD, ED] = other match {
      case grph: OneGraph[VD, ED] => grph
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

    //renumber the indices of the two graphs
    val gr1IndexStart:Int = resolution.numBetween(startBound, span.start)
    val gr2IndexStart:Int = resolution.numBetween(startBound, grp2.span.start)
    val gr1Edges = if (gr1IndexStart > 0) graphs.edges.mapValues{ e => e.attr.map{ case (k,v) => (k + gr1IndexStart, v)}} else graphs.edges
    val gr2Edges = if (gr2IndexStart > 0) grp2.graphs.edges.mapValues{ e => e.attr.map{ case (k,v) => (k + gr2IndexStart, v)}} else grp2.graphs.edges
    val gr1Verts = if (gr1IndexStart > 0) graphs.vertices.mapValues{ (vid:VertexId,vattr:Map[TimeIndex,VD]) => vattr.map{ case (k,v) => (k + gr1IndexStart, v)} } else graphs.vertices
    val gr2Verts = if (gr2IndexStart > 0) grp2.graphs.vertices.mapValues{ (vid:VertexId, vattr:Map[TimeIndex,VD]) => vattr.map{ case (k,v) => (k+gr2IndexStart, v)} } else grp2.graphs.vertices

    //now union
    var target = if (sem == AggregateSemantics.All) 2 else 1
    //this is somewhat complicated. union of vertices produces some duplicates
    //reduceByKey applies a specified function to the attributes, which are maps
    //for each pair of maps, we convert them to sequences 
    //(to not loose the values with the same keys)
    //combine them, group by key, unroll the value, which is (k, list)
    //filter out those where there's not enough values for the type of semantics
    //then reduce with the user-specified vertex function
    //and finally drop vertices that have no remaining elements
    val newverts = (gr1Verts union gr2Verts).reduceByKey{ (a: Map[TimeIndex, VD], b: Map[TimeIndex, VD]) => 
      (a.toSeq ++ b.toSeq).groupBy{case (k,v) => k}.mapValues(v => v.map{case (k,m) => m}).filter{case (index, lst) => lst.size >= target}.mapValues(v => v.reduce(vFunc)).map(identity)
    }.filter{ case (id, attr) => !attr.isEmpty}

    //now similar with edges except that there is no groupbykey for edges directly
    val newedges = (gr1Edges union gr2Edges).map(e => ((e.srcId, e.dstId), e.attr))
      .reduceByKey{ (a: Map[TimeIndex, ED], b: Map[TimeIndex, ED]) =>
      (a.toSeq ++ b.toSeq).groupBy{case (k,v) => k}.mapValues(v => v.map{case (k,m) => m}).filter{case (index, lst) => lst.size >= target}.mapValues(v => v.reduce(eFunc)).map(identity)
    }.filter{e => !e._2.isEmpty}
    .map{ case (k,v) => Edge(k._1, k._2, v)}

    new OneGraph[VD, ED](mergedIntervals, Graph[Map[TimeIndex, VD], Map[TimeIndex, ED]](newverts, newedges))
  }

  override def intersect(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    /** The type checking already validates that the structurally the graphs are union-compatible
      * But we also need to check that they are temporally union-compatible
      * this includes having the same resolution and aligning intervals
      * Two temporal sequences are compatible if their first elements are compatible
      */ 
    var grp2: OneGraph[VD, ED] = other match {
      case grph: OneGraph[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (!intervals.head.isUnionCompatible(grp2.intervals.head)) {
      throw new IllegalArgumentException("two graphs are not union-compatible in the temporal schema")
    }

    //compute the combined span
    val startBound = if (span.start.isBefore(grp2.span.start)) grp2.span.start else span.start
    val endBound = if (span.end.isAfter(grp2.span.end)) grp2.span.end else span.end
    if (startBound.isAfter(endBound) || startBound.isEqual(endBound)) {
      OneGraph.emptyGraph[VD,ED]()
    } else {
      //we are taking a temporal subset of both graphs
      //and then doing the structural part
      val gr1Sel = select(Interval(startBound, endBound))  match {
        case grph: OneGraph[VD, ED] => grph
        case _ => throw new ClassCastException
      }
      val gr2Sel = grp2.select(Interval(startBound, endBound)) match {
        case grph: OneGraph[VD, ED] => grph
        case _ => throw new ClassCastException
      }

      //now union
      var target = if (sem == AggregateSemantics.All) 2 else 1
      //this is somewhat complicated. union of vertices produces some duplicates
      //reduceByKey applies a specified function to the attributes, which are maps
      //for each pair of maps, we convert them to sequences
      //(to not loose the values with the same keys)
      //combine them, group by key, unroll the value, which is (k, list)
      //filter out those where there's not enough values for the type of semantics
      //then reduce with the user-specified vertex function
      //and finally drop vertices that have no remaining elements
      val newverts = (gr1Sel.graphs.vertices union gr2Sel.graphs.vertices).reduceByKey{ (a: Map[TimeIndex, VD], b: Map[TimeIndex, VD]) =>
        (a.toSeq ++ b.toSeq).groupBy{case (k,v) => k}.mapValues(v => v.map{case (k,m) => m}).filter{case (index, lst) => lst.size >= target}.mapValues(v => v.reduce(vFunc)).map(identity)
      }.filter{ case (id, attr) => !attr.isEmpty}

      //now similar with edges
      //except edges have no groupbykey
    val newedges: RDD[Edge[Map[TimeIndex, ED]]] = (gr1Sel.graphs.edges union gr2Sel.graphs.edges).map(e => ((e.srcId, e.dstId), e.attr))
      .reduceByKey{ (a: Map[TimeIndex, ED], b: Map[TimeIndex, ED]) =>
      (a.toSeq ++ b.toSeq).groupBy{case (k,v) => k}
        .mapValues(v => v.map{case (k,m) => m})
        .filter{case (index, lst) => lst.size >= target}
        .mapValues(v => v.reduce(eFunc))
        .map(identity)
      }
      .filter{e => !e._2.isEmpty}
      .map{ case (k,v) => Edge(k._1, k._2, v)}

      new OneGraph[VD, ED](gr2Sel.intervals, Graph[Map[TimeIndex, VD], Map[TimeIndex, ED]](newverts, newedges))
    }
  }

  override def pregel[A: ClassTag]
     (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
       sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): TemporalGraph[VD, ED] = {
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

    val newgrp: Graph[Map[TimeIndex, VD], Map[TimeIndex, ED]] = Pregel(graphs, initM, maxIterations, activeDirection)(vertexP, sendMsgC, mergeMsgC)
    new OneGraph[VD, ED](intervals, newgrp)
  }

  //run pagerank on each interval
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): TemporalGraph[Double, Double] = {
    if (uni) {
      def mergeFunc(a:Map[TimeIndex,Int], b:Map[TimeIndex,Int]): Map[TimeIndex,Int] = {
        a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
      }

      val degrees: VertexRDD[Map[TimeIndex,Int]] = graphs.aggregateMessages[Map[TimeIndex, Int]](
        ctx => {
          ctx.sendToSrc(ctx.attr.mapValues(x => 1).map(identity))
          ctx.sendToDst(ctx.attr.mapValues(x => 1).map(identity))
        },
        mergeFunc, TripletFields.None)

      val pagerankGraph: Graph[Map[TimeIndex,(Double,Double)], Map[TimeIndex,(Double,Double)]] = graphs.outerJoinVertices(degrees) {
        case (vid, vdata, Some(deg)) => deg
        case (vid, vdata, None) =>  vdata.mapValues(x => 0).map(identity)
      }
        .mapTriplets( e =>  e.attr.map { case (k,v) => (k -> (1.0/e.srcAttr(k), 1.0/e.dstAttr(k)))}.map(identity))
        .mapVertices( (id,attr) => attr.mapValues{ x => (0.0,0.0)}.map(identity))
        .cache()

      def vertexProgram(id: VertexId, attr: Map[TimeIndex, (Double,Double)], msg: Map[TimeIndex, Double]): Map[TimeIndex, (Double,Double)] = {
        var vals = attr
        msg.foreach { x =>
          val (k,v) = x
          if (vals.contains(k)) {
            val (oldPR, lastDelta) = vals(k)
            val newPR = oldPR + (1.0 - resetProb) * v
            vals = vals.updated(k,(newPR,newPR-oldPR))
          }
        }
        vals
      }

      def sendMessage(edge: EdgeTriplet[Map[TimeIndex,(Double,Double)], Map[TimeIndex, (Double,Double)]]) = {
        //need to generate an iterator of messages for each index
        val tmp: Buffer[(VertexId,Map[TimeIndex,Double])] = Buffer[(VertexId,Map[TimeIndex,Double])]()
        //if (edge.srcAttr == null)
        //  throw new IllegalArgumentException("edge triplet source attribute is null for edge " + edge.srcId + "-" + edge.dstId)
        //if (edge.dstAttr == null)
        //  throw new IllegalArgumentException("edge triplet dst attribute is null for edge " + edge.srcId + "-" + edge.dstId)
        edge.attr.foreach { case (index,va) =>
          if (edge.srcAttr(index)._2 > tol) {
            tmp += ((edge.dstId, Map((index -> edge.srcAttr(index)._2 * va._1))))
          }
          if (edge.dstAttr(index)._2 > tol) {
            tmp += ((edge.srcId, Map((index -> edge.dstAttr(index)._2 * va._2))))
          }
        }
        tmp.toSeq.groupBy{ case (k,v) => k}
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

      val newgraph: Graph[Map[TimeIndex,Double], Map[TimeIndex,Double]] = Pregel(pagerankGraph, initialMessage, numIter, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)
      .mapTriplets(e => e.attr.mapValues(x => x._1))
      .mapVertices((vid,attr) => attr.mapValues(x => x._1))

      new OneGraph[Double, Double](intervals, newgraph)

    } else {
      //TODO: implement this using pregel
      throw new UnsupportedOperationException("directed version of pageRank not yet implemented")
    }
  }
  
  override def degree(): TemporalGraph[Double, Double] = {
      def mergeFunc(a:Map[TimeIndex,Double], b:Map[TimeIndex,Double]): Map[TimeIndex,Double] = {
        a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0.0)) }
      }

      val degrees: VertexRDD[Map[TimeIndex,Double]] = graphs.aggregateMessages[Map[TimeIndex, Double]](
        ctx => {
          ctx.sendToSrc(ctx.attr.mapValues(x => 1.0).map(identity))
          ctx.sendToDst(ctx.attr.mapValues(x => 1.0).map(identity))
        },
        mergeFunc, TripletFields.None)

    val newgraph: Graph[Map[TimeIndex,Double], Map[TimeIndex,Double]] = graphs.outerJoinVertices(degrees) {
      case (vid, vdata, Some(deg)) => deg
      case (vid, vdata, None) => vdata.mapValues(x => 0.0).map(identity)
    }
      .mapEdges(e => e.attr.map{ case (k,v) => (k -> 0.0)}.map(identity))

    new OneGraph[Double, Double](intervals, newgraph)
  }

  //run connected components on each interval
  override def connectedComponents(): TemporalGraph[VertexId, ED] = {
    //TODO: implement this using pregel
    throw new UnsupportedOperationException("directed version of pageRank not yet implemented")
   }
  
  //run shortestPaths on each interval
  override def shortestPaths(landmarks: Seq[VertexId]): TemporalGraph[ShortestPathsXT.SPMap, ED] = {
    //TODO: implement this using pregel
    throw new UnsupportedOperationException("directed version of pageRank not yet implemented")
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    if (graphs.edges.isEmpty)
      0
    else
      graphs.edges.partitions.size
  }

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): TemporalGraph[VD, ED] = {
    //just persist the graph itself
    graphs.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): TemporalGraph[VD, ED] = {
    graphs.unpersist(blocking)
    this
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int): TemporalGraph[VD, ED] = {
    partitionBy(pst, runs, graphs.edges.partitions.size)
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): TemporalGraph[VD, ED] = {
    val numParts = if (parts > 0) parts else graphs.edges.partitions.size

    if (pst != PartitionStrategyType.None) {
      //not changing the intervals
      new OneGraph[VD, ED](intervals, graphs.partitionByExt(PartitionStrategies.makeStrategy(pst, 0, intervals.size, runs), numParts))
    } else
      this
  }

}

object OneGraph {
  final def loadData(dataPath: String, start: LocalDate, end: LocalDate): OneGraph[String, Int] = {
    loadWithPartition(dataPath, start, end, PartitionStrategyType.None, 1)
  }

  final def loadWithPartition(dataPath: String, start: LocalDate, end: LocalDate, strategy: PartitionStrategyType.Value, runWidth: Int): OneGraph[String, Int] = {
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

    var intvs: Seq[Interval] = Seq[Interval]()
    var xx: LocalDate = minDate
    var count:Int = 0
    while (xx.isBefore(maxDate)) {
      intvs = intvs :+ res.getInterval(xx)
      count += 1
      xx = intvs.last.end
    }

    var users: RDD[(VertexId,Map[Int,String])] = MultifileLoad.readNodes(dataPath, minDate, intvs.last.start).flatMap{ x => 
      val (filename, line) = x
      val dt = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))
      val parts = line.split(",")
      val index = res.numBetween(minDate, dt)
      if (parts.size > 1 && parts.head != "" && index > -1) {
        Some((parts.head.toLong, Map(index -> parts(1).toString)))
      } else None
    }.reduceByKey{ (a:Map[Int, String], b:Map[Int, String]) => a ++ b}

    val in = MultifileLoad.readEdges(dataPath, minDate, intvs.last.start)
      .flatMap{ x =>
      val (filename, line) = x
      val dt = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))
      if (!line.isEmpty && line(0) != '#') {
        val lineArray = line.split("\\s+")
        val srcId = lineArray(0).toLong
        val dstId = lineArray(1).toLong
        var attr = 0
        if(lineArray.length > 2){
          attr = lineArray{2}.toInt
        }
        val index = res.numBetween(minDate, dt)
        if (srcId > dstId)
          Some((dstId, srcId), Map(index -> attr))
        else
          Some((srcId, dstId), Map(index -> attr))
      } else None
    }.reduceByKey{ (a, b) => a ++ b}

    val edges = EdgeRDD.fromEdges[Map[TimeIndex, Int], Map[TimeIndex, String]](in.map{ case (k,v) => Edge(k._1, k._2, v)})

    var graph: Graph[Map[Int, String], Map[Int, Int]] = Graph(users, edges, Map[Int,String]())

    if (strategy != PartitionStrategyType.None) {
      val numParts = graph.edges.partitions.size
      graph = graph.partitionByExt(PartitionStrategies.makeStrategy(strategy, 0, intvs.size, runWidth), numParts)
    }    

    new OneGraph[String, Int](intvs, graph.persist())
  }

  def emptyGraph[VD: ClassTag, ED: ClassTag]():OneGraph[VD, ED] = new OneGraph(Seq[Interval](), Graph[Map[TimeIndex, VD],Map[TimeIndex, ED]](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD))

}
