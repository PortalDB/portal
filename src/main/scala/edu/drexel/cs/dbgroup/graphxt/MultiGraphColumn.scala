//Multigraph with column store for attributes
package edu.drexel.cs.dbgroup.graphxt

import scala.collection.immutable.BitSet
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.collection.breakOut

import scala.reflect.ClassTag
import scala.util.control._

import java.time.LocalDate

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.rdd._

import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.impl.GraphXPartitionExtension._

import edu.drexel.cs.dbgroup.graphxt.util.MultifileLoad

class MultiGraphColumn[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], grs: Graph[BitSet, (TimeIndex, ED)], veratts: RDD[((VertexId,TimeIndex),VD)]) extends TemporalGraph[VD, ED] with Serializable {
  val graphs: Graph[BitSet, (TimeIndex, ED)] = grs
  val resolution:Resolution = if (intvs.size > 0) intvs.head.resolution else Resolution.zero

  intvs.foreach { ii =>
    if (!ii.resolution.isEqual(resolution))
      throw new IllegalArgumentException("temporal sequence is not valid, intervals are not all of equal resolution")
  }

  val intervals: Seq[Interval] = intvs
  lazy val span: Interval = if (intervals.size > 0) Interval(intervals.head.start, intervals.last.end) else Interval(LocalDate.now, LocalDate.now)

  //vertex attributes are kept in a separate rdd with an id,time key
  val vertexattrs: RDD[((VertexId,TimeIndex),VD)] = veratts

  /** Default constructor is provided to support serialization */
  protected def this() = this(Seq[Interval](), Graph[BitSet, (TimeIndex, ED)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD), ProgramContext.sc.emptyRDD)

  override def size(): Int = intervals.size

  override def vertices: VertexRDD[Map[Interval, VD]] = {
    val start = span.start
    VertexRDD(vertexattrs.map{ case (k,v) => (k._1, Map[Interval,VD](resolution.getInterval(start, k._2) -> v))}
    .reduceByKey((a: Map[Interval, VD], b: Map[Interval, VD]) => a ++ b))
  }

  override def verticesFlat: VertexRDD[(Interval, VD)] = {
    val start = span.start
    VertexRDD(vertexattrs.map{ case (k,v) => (k._1, (resolution.getInterval(start, k._2), v))})
  }

  override def edges: EdgeRDD[Map[Interval, ED]] = {
    val start = span.start
    EdgeRDD.fromEdges[Map[Interval, ED], VD](graphs.edges.map(x => ((x.srcId, x.dstId), Map[Interval, ED](resolution.getInterval(start, x.attr._1) -> x.attr._2)))
    .reduceByKey((a: Map[Interval, ED], b: Map[Interval, ED]) => a ++ b)
    .map(x => Edge(x._1._1, x._1._2, x._2))
    )
  }

  override def edgesFlat: EdgeRDD[(Interval, ED)] = {
    val start = span.start
    graphs.edges.mapValues(e => (resolution.getInterval(start, e.attr._1), e.attr._2))
  }

  override def degrees: VertexRDD[Map[Interval, Int]] = {
    def mergedFunc(a:Map[TimeIndex,Int], b:Map[TimeIndex,Int]): Map[TimeIndex,Int] = {
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    val start = span.start

    //compute degree of each vertex for each interval
    //this should produce a map from interval to degree for each vertex
    //since edge is treated by MultiGraph as undirectional, can use the edge markings
    graphs.aggregateMessages[Map[TimeIndex,Int]](
      ctx => {
        ctx.sendToSrc(Map(ctx.attr._1 -> 1))
        ctx.sendToDst(Map(ctx.attr._1 -> 1))
      },
      mergedFunc,
      TripletFields.All)
    .mapValues(v => v.map(x => (resolution.getInterval(start, x._1) -> x._2)))
  }

  override def select(bound: Interval): TemporalGraph[VD, ED] = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this

    if (span.intersects(bound)) {
      val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
      val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end

      //compute indices of start and stop
      val selectStart:Int = intervals.indexOf(resolution.getInterval(startBound))
      var selectStop:Int = intervals.indexOf(resolution.getInterval(endBound))
      if (selectStop < 0) selectStop = intervals.size

      val newIntvs: Seq[Interval] = intervals.slice(selectStart, selectStop)

      //make a bitset that represents the selected years only
      val mask:BitSet = BitSet((selectStart to selectStop): _*)
      val subg = graphs.subgraph(
        vpred = (vid, attr) => !(attr & mask).isEmpty,
        epred = et => et.attr._1 >= selectStart && et.attr._1 < selectStop)

      //now need to update indices
      val resg = subg.mapVertices((vid, vattr) => vattr.filter(x => x >= selectStart && x < selectStop).map(_ - selectStart)).mapEdges(e => (e.attr._1 - selectStart, e.attr._2))

      //now need to update the vertex attribute rdd
      val attrs = vertexattrs.filter{ case (k,v) => k._2 >= selectStart && k._2 <= selectStop}.map{ case (k,v) => ((k._1, k._2 - selectStart), v)}

      new MultiGraphColumn[VD, ED](newIntvs, resg, attrs)

    } else
      MultiGraphColumn.emptyGraph()
  }

  override def select(tpred: Interval => Boolean): TemporalGraph[VD, ED] = {
    //make a bitset that represents the selected years only
    val chosen:ListBuffer[Int] = ListBuffer[Int]()
    intervals.zipWithIndex.foreach{ case (p,i) => if (tpred(p)) chosen += i} 
    val mask:BitSet = BitSet() ++ chosen
    val start = span.start

    new MultiGraphColumn[VD, ED](intervals, graphs.subgraph(
      vpred = (vid, attr) => !(attr & mask).isEmpty,
      epred = et => mask(et.attr._1))
      .mapVertices((vid, vattr) => vattr & mask)
    , vertexattrs.filter{ case (k,v) => tpred(resolution.getInterval(start, k._2))}
    )
  }

  override def select(epred: EdgeTriplet[VD,ED] => Boolean, vpred: (VertexId, VD) => Boolean): TemporalGraph[VD, ED] = {
    //TODO: implement this
      throw new UnsupportedOperationException("this version of select not yet implementet")
  }

  override def aggregate(res: Resolution, sem: AggregateSemantics.Value, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
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
      val expected:Integer = res.getNumParts(resolution, intv.start)

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

    val broadcastIndMap = ProgramContext.sc.broadcast(indMap)

    //for each vertex, create a new bitset
    //then filter out those where bitset is all unset
    val parts:Iterable[Int] = cntMap.values
    val wverts: Graph[BitSet, (TimeIndex,ED)] = graphs.mapVertices { (vid, attr) =>
      var total:Int = 0
      BitSet() ++ parts.zipWithIndex.flatMap { case (expected,index) =>    //produce indices that should be set
        //make a mask for this part
        val mask = BitSet((total to (total+expected)): _*)
        if (sem == AggregateSemantics.Universal) {
          if (mask.subsetOf(attr))
            Some(index)
          else
            None
        } else if (sem == AggregateSemantics.Existential) {
          if (!(mask & attr).isEmpty)
            Some(index)
          else
            None
        } else None
      }}

    var edges: EdgeRDD[(TimeIndex, ED)] = null
    if (sem == AggregateSemantics.Existential) {
      edges = EdgeRDD.fromEdges[(TimeIndex, ED), VD](wverts.edges.map(x => ((x.srcId, x.dstId, broadcastIndMap.value(x.attr._1)), x.attr._2)).reduceByKey(eAggFunc).map { x =>
        val (k, v) = x
        //key is srcid, dstid, resolution, value is attrib
        Edge(k._1, k._2, (k._3, v))
      })
    } else if (sem == AggregateSemantics.Universal) {
      edges = EdgeRDD.fromEdges[(TimeIndex, ED), VD](wverts.edges.map(x => ((x.srcId, x.dstId, broadcastIndMap.value(x.attr._1)), (x.attr._2, 1))).reduceByKey((x, y) => (eAggFunc(x._1, y._1), x._2 + y._2)).filter { case (k, (attr, cnt)) => cnt == cntMap(k._3) }.map { x =>
        val (k, v) = x
        //key is srcid, dstid, resolution, value is attrib and count
        Edge(k._1, k._2, (k._3, v._1))
      })
    }

    //TODO: see if filtering can be done more efficiently
    val attrs = if (sem == AggregateSemantics.Universal) vertexattrs.map{ case (k,v) => ((k._1, broadcastIndMap.value(k._2)), (v, 1))}.reduceByKey((x,y) => (vAggFunc(x._1, y._1), x._2 + y._2)).filter{ case (k, (attr,cnt)) => cnt == cntMap(k._2)}.map{ case (k,v) => (k, v._1)} else vertexattrs.map{ case (k,v) => ((k._1, broadcastIndMap.value(k._2)), v)}.reduceByKey(vAggFunc)

    broadcastIndMap.destroy()

    new MultiGraphColumn[VD, ED](intvs, Graph(wverts.vertices, edges), attrs)
  }

  override def transform[ED2: ClassTag, VD2: ClassTag](emap: (Edge[ED], Interval) => ED2, vmap: (VertexId, Interval, VD) => VD2): TemporalGraph[VD2, ED2] = {
    val start = span.start
    new MultiGraphColumn[VD2, ED2](intervals, graphs.mapEdges{ e: Edge[(TimeIndex, ED)] => (e.attr._1, emap(Edge(e.srcId, e.dstId, e.attr._2), resolution.getInterval(start, e.attr._1)))}, vertexattrs.map{ case (k,v) => (k, vmap(k._1, resolution.getInterval(start, k._2), v))})
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val start = span.start
    new MultiGraphColumn[VD2, ED](intervals, graphs, vertexattrs.map{ case (k,v) => (k, map(k._1, resolution.getInterval(start, k._2), v))})
  }

  override def mapEdges[ED2: ClassTag](map: (Edge[ED], Interval) => ED2): TemporalGraph[VD, ED2] = {
    val start = span.start
    new MultiGraphColumn[VD, ED2](intervals, graphs.mapEdges{ e: Edge[(TimeIndex, ED)] =>
      (e.attr._1, map(Edge(e.srcId, e.dstId, e.attr._2), resolution.getInterval(start, e.attr._1)))
    }
    , vertexattrs)
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, Map[Interval, U])])(mapFunc: (VertexId, Interval, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val start = span.start

    val in: RDD[((VertexId,TimeIndex),U)] = other.flatMap(x => x._2.map(y => ((x._1, resolution.numBetween(start, y._1.start)), y._2)))

    new MultiGraphColumn[VD2, ED](intervals, graphs, vertexattrs.leftOuterJoin(in).map{ case (k,v) => (k, mapFunc(k._1, resolution.getInterval(start, k._2), v._1, v._2))})
  }

  override def union(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    /** The type checking already validates that the structurally the graphs are union-compatible
      * But we also need to check that they are temporally union-compatible
      * this includes having the same resolution and aligning intervals
      * Two temporal sequences are compatible if their first elements are compatible
      */ 
    val grp2: MultiGraphColumn[VD, ED] = other match {
      case grph: MultiGraphColumn[VD, ED] => grph
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
    val gr1Edges = if (gr1IndexStart > 0) graphs.edges.mapValues{ e => (e.attr._1 + gr1IndexStart, e.attr._2)  } else graphs.edges
    val gr2Edges = if (gr2IndexStart > 0) grp2.graphs.edges.mapValues{ e => (e.attr._1 + gr2IndexStart, e.attr._2) } else grp2.graphs.edges
    val gr1Verts = if (gr1IndexStart > 0) graphs.vertices.mapValues{ (vid:VertexId,vattr:BitSet) => vattr.map(_ + gr1IndexStart)} else graphs.vertices
    val gr2Verts = if (gr2IndexStart > 0) grp2.graphs.vertices.mapValues{ (vid:VertexId, vattr:BitSet) => vattr.map(_ + gr2IndexStart)} else grp2.graphs.vertices

    //now union
    var target = if (sem == AggregateSemantics.Universal) 2 else 1

    //we want to keep those vertices that exist either in either or both graphs
    //depending on the semantics
    val newverts = if (sem == AggregateSemantics.Universal) (gr1Verts union gr2Verts).reduceByKey{ (a: BitSet, b: BitSet) => a & b }.filter{ case (vid, vattr) => !vattr.isEmpty} else (gr1Verts union gr2Verts).reduceByKey{ (a: BitSet, b: BitSet) => a.union(b)  }

    //this is somewhat complicated. union of edges produces some duplicates
    //reduceByKey applies a specified function to the attributes, which are (index,value)
    //filter out those where there's not enough values for the type of semantics
    //then reduce with the user-specified edge function
    //and drop edges that have no remaining elements
    val newedges = (gr1Edges union gr2Edges).map(x => ((x.srcId, x.dstId, x.attr._1), (x.attr._2, 1)))
      .reduceByKey((e1,e2) => (eFunc(e1._1, e2._1), e1._2 + e2._2))
      .filter{ case (k, (attr, cnt)) => cnt >= target}
      .map{ case (k,v) => Edge(k._1, k._2, (k._3, v._1))}

    val gr1attrs = if (gr1IndexStart > 0) vertexattrs.map{ case (k,v) => ((k._1, k._2 + gr1IndexStart), v)} else vertexattrs
    val gr2attrs = if (gr2IndexStart > 0) grp2.vertexattrs.map{ case (k,v) => ((k._1, k._2 + gr2IndexStart), v)} else grp2.vertexattrs
    //now put them together
    val newattrs = if (sem == AggregateSemantics.Universal) (gr1attrs join gr2attrs).mapValues(x => vFunc(x._1, x._2)) else (gr1attrs union gr2attrs).reduceByKey(vFunc)

    new MultiGraphColumn[VD, ED](mergedIntervals, Graph[BitSet, (TimeIndex, ED)](newverts, EdgeRDD.fromEdges[(TimeIndex, ED),BitSet](newedges)), newattrs)
  }

  override def intersection(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    /** The type checking already validates that the structurally the graphs are union-compatible
      * But we also need to check that they are temporally union-compatible
      * this includes having the same resolution and aligning intervals
      * Two temporal sequences are compatible if their first elements are compatible
      */ 
    val grp2: MultiGraphColumn[VD, ED] = other match {
      case grph: MultiGraphColumn[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (!intervals.head.isUnionCompatible(grp2.intervals.head)) {
      throw new IllegalArgumentException("two graphs are not union-compatible in the temporal schema")
    }

    //compute the combined span
     val startBound = if (span.start.isBefore(grp2.span.start)) grp2.span.start else span.start
    val endBound = if (span.end.isAfter(grp2.span.end)) grp2.span.end else span.end
    if (startBound.isAfter(endBound) || startBound.isEqual(endBound)) {
      MultiGraphColumn.emptyGraph()
    } else {
      //we are taking a temporal subset of both graphs
      //and then doing the structural part
      val gr1Sel = select(Interval(startBound, endBound)) match {
        case grph: MultiGraphColumn[VD, ED] => grph
        case _ => throw new ClassCastException
      }
      val gr2Sel = grp2.select(Interval(startBound, endBound)) match {
        case grph: MultiGraphColumn[VD, ED] => grph
        case _ => throw new ClassCastException
      }

      //we want to keep those vertices that exist either in either or both graphs
      //depending on the semantics
      val newverts = if (sem == AggregateSemantics.Universal) (gr1Sel.graphs.vertices union gr2Sel.graphs.vertices).reduceByKey{ (a: BitSet, b: BitSet) => a & b }.filter{ case (vid, vattr) => !vattr.isEmpty} else (gr1Sel.graphs.vertices union gr2Sel.graphs.vertices).reduceByKey{ (a: BitSet, b: BitSet) => a.union(b)  }

      //this is somewhat complicated. union of edges produces some duplicates
      //reduceByKey applies a specified function to the attributes, which are (index,value)
      //filter out those where there's not enough values for the type of semantics
      //then reduce with the user-specified edge function
      //and drop edges that have no remaining elements
      var target = if (sem == AggregateSemantics.Universal) 2 else 1
      val newedges = (gr1Sel.graphs.edges union gr2Sel.graphs.edges).map(x => ((x.srcId, x.dstId, x.attr._1), (x.attr._2, 1)))
        .reduceByKey((e1,e2) => (eFunc(e1._1, e2._1), e1._2 + e2._2))
        .filter{ case (k, (attr, cnt)) => cnt >= target}
        .map{ case (k,v) => Edge(k._1, k._2, (k._3, v._1))}

      val newattrs = if (sem == AggregateSemantics.Universal) (gr1Sel.vertexattrs join gr2Sel.vertexattrs).mapValues(x => vFunc(x._1, x._2)) else (gr1Sel.vertexattrs union gr2Sel.vertexattrs).reduceByKey(vFunc)

      new MultiGraphColumn[VD, ED](gr1Sel.intervals, Graph[BitSet, (TimeIndex, ED)](newverts, EdgeRDD.fromEdges[(TimeIndex,ED), BitSet](newedges)), newattrs)
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
    def sendMsgC(edge: EdgeTriplet[Map[TimeIndex, VD], (TimeIndex, ED)]): Iterator[(VertexId, Map[TimeIndex, A])] = {
      val et = new EdgeTriplet[VD, ED]
      et.srcId = edge.srcId
      et.dstId = edge.dstId
      et.srcAttr = edge.srcAttr.get(edge.attr._1).get
      et.dstAttr = edge.dstAttr.get(edge.attr._1).get
      et.attr = edge.attr._2
      //this returns Iterator[(VertexId, A)], but we need
      //Iterator[(VertexId, Map[TimeIndex, A])]
      sendMsg(et).map(x => (x._1, Map[TimeIndex, A](edge.attr._1 -> x._2)))
    }
    def mergeMsgC(a: Map[TimeIndex, A], b: Map[TimeIndex, A]): Map[TimeIndex, A] = {
      (a.keySet ++ b.keySet).map { k =>
        k -> mergeMsg(a.getOrElse(k, defValue), b.getOrElse(k, defValue))
      }.toMap
    }

    //need to put values into vertices
    val grph = graphs.outerJoinVertices(vertexattrs.map{ case (k,v) => (k._1, Map[TimeIndex, VD](k._2 -> v))}.reduceByKey((a, b) => a ++ b))((vid, bitst, mp) => mp.get)

    val newgrp: Graph[Map[TimeIndex, VD], (TimeIndex, ED)] = Pregel(grph, initM, maxIterations, activeDirection)(vertexP, sendMsgC, mergeMsgC)
    //need to convert back to bitmap and vertexattrs
    //FIXME:? is it ok that we are throwing away the new edge values?
    val newattrs: RDD[((VertexId, TimeIndex), VD)] = newgrp.vertices.flatMap{ case (vid, vattr) => vattr.map{ case (k,v) => ((vid, k),v)  }}
    new MultiGraphColumn[VD, ED](intervals, graphs, newattrs)
  }

  //run pagerank on each interval
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): TemporalGraph[Double, Double] = {
    //the values of the vertices don't matter
    var i:Int = 0
    val initialMessage:Map[TimeIndex,Double] = (for(i <- 0 to intervals.size) yield (i -> resetProb / (1.0 - resetProb)))(breakOut)

    def mergedFunc(a:Map[TimeIndex,Int], b:Map[TimeIndex,Int]): Map[TimeIndex,Int] = {
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    //compute degree of each vertex for each interval
    //this should produce a map from interval to degree for each vertex
    val degrees: VertexRDD[Map[TimeIndex,Int]] = graphs.aggregateMessages[Map[TimeIndex,Int]](
      ctx => { 
        ctx.sendToSrc(Map(ctx.attr._1 -> 1))
        if (uni)
          ctx.sendToDst(Map(ctx.attr._1 -> 1))
      },
      mergedFunc,
      TripletFields.None)

    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/degree and each vertex with attribute 1.0.
    val pagerankGraph: Graph[Map[TimeIndex,(Double,Double)], (TimeIndex,Double,Double)] = graphs
      // Associate the degree with each vertex for each interval
      .outerJoinVertices(degrees) {
      case (vid, vdata, Some(deg)) => deg
      case (vid, vdata, None) => Map[TimeIndex,Int]()
      }
      // Set the weight on the edges based on the degree of that interval
      .mapTriplets( e => (e.attr._1, 1.0 / e.srcAttr(e.attr._1), 1.0 / e.dstAttr(e.attr._1)) )
      // Set the vertex attributes to (initalPR, delta = 0) for each interval
      .mapVertices( (id, attr) => attr.mapValues { x => (0.0,0.0) } )
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: Map[TimeIndex,(Double, Double)], msg: Map[TimeIndex,Double]): Map[TimeIndex,(Double, Double)] = {
      //need to compute new values for each interval
      //each edge carries a message for one interval,
      //which are combined by the combiner into a hash
      //for each interval in the msg hash, update
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

    def sendMessage(edge: EdgeTriplet[Map[TimeIndex,(Double, Double)], (TimeIndex, Double, Double)]) = {
      //each edge attribute is supposed to be a triple of (year index, 1/degree, 1/degree)
      //each vertex attribute is supposed to be a map of (double,double) for each index
      if (uni && edge.srcAttr(edge.attr._1)._2 > tol && 
        edge.dstAttr(edge.attr._1)._2 > tol) {
        Iterator((edge.dstId, Map((edge.attr._1 -> edge.srcAttr(edge.attr._1)._2 * edge.attr._2))), (edge.srcId, Map((edge.attr._1 -> edge.dstAttr(edge.attr._1)._2 * edge.attr._3))))
      } else if (edge.srcAttr(edge.attr._1)._2 > tol) { 
        Iterator((edge.dstId, Map((edge.attr._1 -> edge.srcAttr(edge.attr._1)._2 * edge.attr._2))))
      } else if (uni && edge.dstAttr(edge.attr._1)._2 > tol) {
        Iterator((edge.srcId, Map((edge.attr._1 -> edge.dstAttr(edge.attr._1)._2 * edge.attr._3))))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Map[TimeIndex,Double], b: Map[TimeIndex,Double]): Map[TimeIndex,Double] = {
     (a.keySet ++ b.keySet).map { i =>
        val count1Val:Double = a.getOrElse(i, 0.0)
        val count2Val:Double = b.getOrElse(i, 0.0)
        i -> (count1Val + count2Val)
      }.toMap
     }

    val direction = if (uni) EdgeDirection.Either else EdgeDirection.Out
    val resultGraph =     // Execute a dynamic version of Pregel.
    Pregel(pagerankGraph, initialMessage, numIter,
      activeDirection = direction)(vertexProgram, sendMessage, messageCombiner)
      .mapTriplets(e => (e.attr._1, e.attr._2)) //I don't think it matters which we pick

    //now need to extract the values into a separate rdd again
    val newattrs = resultGraph.vertices.flatMap{ case (vid,vattr) => vattr.map{ case (k,v) => ((vid,k), v._1)}}

    new MultiGraphColumn[Double,Double](intervals, resultGraph.outerJoinVertices(graphs.vertices)((vid, vl, bt) => bt.get), newattrs)

  }

  //run connected components on each interval
  override def connectedComponents(): TemporalGraph[VertexId, ED] = {
    val ccGraph: Graph[Map[TimeIndex, VertexId], (TimeIndex, ED)] = graphs
    // Set the vertex attributes to vertex id for each interval
      .mapVertices((id, attr) => attr.toSeq.map(x => (x -> id)).toMap)
      .cache();

    // Define the three functions needed to implement ConnectedComponents in the GraphX version of Pregel
    def vertexProgram(id: VertexId, attr: Map[TimeIndex, VertexId], msg: Map[TimeIndex, VertexId]): Map[TimeIndex, VertexId] = {
      //need to compute new values for each interval
      //each edge carries a message for one interval,
      //which are combined by the combiner into a hash
      //for each interval in the msg hash, update
      var vals = attr
      msg.foreach { x =>
        val (k, v) = x
        if (vals.contains(k)) {
          val cc = math.min(attr(k), msg(k))
          vals = vals.updated(k, cc)
        }
      }
      vals
    }

    def sendMessage(edge: EdgeTriplet[Map[TimeIndex, VertexId], (TimeIndex, ED)]): Iterator[(VertexId, Map[TimeIndex, VertexId])] = {
      //each vertex attribute is supposed to be a map of int->int for each index
      var yearIndex = edge.attr._1

      if (edge.srcAttr(yearIndex) < edge.dstAttr(yearIndex)) {
        Iterator((edge.dstId, Map(yearIndex -> edge.srcAttr(yearIndex))))
      } else if (edge.srcAttr(yearIndex) > edge.dstAttr(yearIndex)) {
        Iterator((edge.srcId, Map(yearIndex -> edge.dstAttr(yearIndex))))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Map[TimeIndex, VertexId], b: Map[TimeIndex, VertexId]): Map[TimeIndex, VertexId] = {
      (a.keySet ++ a.keySet).map { i =>
        i -> math.min(a.getOrElse(i, Long.MaxValue), b.getOrElse(i, Long.MaxValue))
      }.toMap
    }

    // The initial message received by all vertices in ConnectedComponents
    val initialMessage: Map[TimeIndex, VertexId] = (for (i <- 0 to intervals.size) yield (i -> Long.MaxValue))(breakOut)

    // Execute a dynamic version of Pregel.
    val result = Pregel(ccGraph, initialMessage,
      activeDirection = EdgeDirection.Either)(
      vertexProgram, sendMessage, messageCombiner)

    val newattrs = result.vertices.flatMap{ case (vid,vattr) => vattr.map{ case (k,v) => ((vid, k), v)}}

    new MultiGraphColumn[VertexId, ED](intervals, result.outerJoinVertices(graphs.vertices)((vid,vl,bt) => bt.get), newattrs)
  }

  //run shortestPaths on each interval
  override def shortestPaths(landmarks: Seq[VertexId]): TemporalGraph[ShortestPathsXT.SPMap, ED] = {
    def makeMap(x: (VertexId, Int)*) = Map(x: _*)
    type SPMap = Map[VertexId, Int]

    def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }

    val spGraph: Graph[Map[TimeIndex, SPMap], (TimeIndex, ED)] = graphs
    // Set the vertex attributes to vertex id for each interval
      .mapVertices { (vid, attr) =>
      attr.toSeq.map{x => 
        if (landmarks.contains(vid))
          (x -> makeMap(vid -> 0))
        else
          (x -> makeMap())
      }.toMap
    }

    def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
      (spmap1.keySet ++ spmap2.keySet).map {
        k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
      }.toMap

    val initialMessage: Map[TimeIndex, SPMap] = (for (i <- 0 to intervals.size) yield (i -> makeMap()))(breakOut)

    def addMapsCombined(a: Map[TimeIndex, SPMap], b: Map[TimeIndex, SPMap]): Map[TimeIndex, SPMap] = {
      (a.keySet ++ b.keySet).map { k =>
        k -> addMaps(a.getOrElse(k, makeMap()), b.getOrElse(k, makeMap()))
      }.toMap
    }

    def vertexProgram(id: VertexId, attr: Map[TimeIndex, SPMap], msg: Map[TimeIndex, SPMap]): Map[TimeIndex, SPMap] = {
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

    def sendMessage(edge: EdgeTriplet[Map[TimeIndex, SPMap], (TimeIndex, ED)]): Iterator[(VertexId, Map[TimeIndex, SPMap])] = {
      //each vertex attribute is supposed to be a map of int->spmap for each index
      var yearIndex = edge.attr._1
      var srcSpMap = edge.srcAttr(yearIndex)
      var dstSpMap = edge.dstAttr(yearIndex)

      val newAttr = incrementMap(dstSpMap)
      val newAttr2 = incrementMap(srcSpMap)

      if (srcSpMap != addMaps(newAttr, srcSpMap))
        Iterator((edge.srcId, Map(yearIndex -> newAttr)))
      else if (dstSpMap != addMaps(newAttr2, dstSpMap))
        Iterator((edge.dstId, Map(yearIndex -> newAttr2)))
      else
        Iterator.empty
    }

    val result = Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMapsCombined)

    val newattrs = result.vertices.flatMap{ case (vid,vattr) => vattr.map{ case (k,v) => ((vid, k), v)}}

    new MultiGraphColumn[ShortestPathsXT.SPMap, ED](intervals, result.outerJoinVertices(graphs.vertices)((vid,vl,bt) => bt.get), newattrs)
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
    var numParts = if (parts > 0) parts else graphs.edges.partitions.size

    if (pst != PartitionStrategyType.None) {
      //not changing the intervals
      new MultiGraphColumn[VD, ED](intervals, graphs.partitionByExt(PartitionStrategies.makeStrategy(pst, 0, intervals.size, runs), numParts), vertexattrs)
    } else
      this
  }

}

object MultiGraphColumn {
  final def loadData(dataPath: String, start: LocalDate, end: LocalDate): MultiGraphColumn[String, Int] = {
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

    val users: RDD[((VertexId,TimeIndex),String)] = MultifileLoad.readNodes(dataPath, minDate, intvs.last.start).flatMap{ x =>
      val (filename, line) = x
      val dt = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))
      val parts = line.split(",")
      val index = res.numBetween(minDate, dt)
      if (parts.size > 1 && parts.head != "" && index > -1) {
        Some((parts.head.toLong, index), parts(1).toString)
      } else None
    }

    val in = MultifileLoad.readEdges(dataPath, minDate, intvs.last.start)
    val edges = GraphLoaderAddon.edgeListFiles(in, res.period, res.unit, minDate, true).edges

    val verts: RDD[(VertexId, BitSet)] = users.map{ case (k,v) => (k._1, BitSet(k._2))}.reduceByKey((a,b) => a union b )

    val graph: Graph[BitSet, (Int, Int)] = Graph(verts, edges, BitSet())

    edges.unpersist()

    new MultiGraphColumn[String, Int](intvs, graph, users)
  }

  def emptyGraph[VD: ClassTag, ED: ClassTag]():MultiGraphColumn[VD, ED] = new MultiGraphColumn(Seq[Interval](), Graph[BitSet,(TimeIndex, ED)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD), ProgramContext.sc.emptyRDD)

}
