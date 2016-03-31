//One graph, but with attributes stored separately
//Each vertex and edge has a value attribute associated with each time period
package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.mutable.HashMap
import scala.collection.immutable.BitSet
import scala.collection.mutable.ListBuffer
import scala.collection.breakOut

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
import edu.drexel.cs.dbgroup.temporalgraph.util.MultifileLoad
import java.time.LocalDate

class OneGraphColumn[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], grs: Graph[BitSet, BitSet], veratts: RDD[(VertexId,(TimeIndex,VD))], edgatts: RDD[((VertexId,VertexId),(TimeIndex,ED))]) extends TemporalGraph[VD, ED] with Serializable {
  val graphs: Graph[BitSet, BitSet] = grs
  val resolution:Resolution = if (intvs.size > 0) intvs.head.resolution else Resolution.zero

  intvs.foreach { ii =>
    if (!ii.resolution.isEqual(resolution))
      throw new IllegalArgumentException("temporal sequence is not valid, intervals are not all of equal resolution")
  }

  val intervals: Seq[Interval] = intvs
  lazy val span: Interval = if (intervals.size > 0) Interval(intervals.head.start, intervals.last.end) else Interval(LocalDate.now, LocalDate.now)

  //vertex attributes are kept in a separate rdd with an id,time key
  val vertexattrs: RDD[(VertexId,(TimeIndex,VD))] = veratts

  //edge attributes are kept in a separate rdd with an id,id,time key
  val edgeattrs: RDD[((VertexId,VertexId),(TimeIndex,ED))] = edgatts

  /** Default constructor is provided to support serialization */
  protected def this() = this(Seq[Interval](), Graph[BitSet,BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD), ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)

  override def size(): Int = intervals.size

  override def materialize() = {
    graphs.numVertices
    graphs.numEdges
  }

  override def vertices: RDD[(VertexId,Map[Interval, VD])] = {
    val start = span.start
    val res = resolution

    vertexattrs.map{ case (k, v) => (k, Map[Interval,VD](res.getInterval(start, v._1) -> v._2))}.reduceByKey{case (a,b) => a ++ b}
  }

  override def verticesFlat: RDD[(VertexId,(Interval, VD))] = {
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
    def mergedFunc(a:Map[TimeIndex,Int], b:Map[TimeIndex,Int]): Map[TimeIndex,Int] = {
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    //compute degree of each vertex for each interval
    //this should produce a map from interval to degree for each vertex
    val start = span.start
    val res = resolution
    graphs.aggregateMessages[Map[TimeIndex,Int]](
      ctx => {
        ctx.sendToSrc(ctx.attr.seq.map(x => (x,1)).toMap)
        ctx.sendToDst(ctx.attr.seq.map(x => (x,1)).toMap)
      },
      mergedFunc,
      TripletFields.None)
    .mapValues(v => v.map(x => (res.getInterval(start, x._1) -> x._2)))
  }

  override def getTemporalSequence: Seq[Interval] = intervals

  override def getSnapshot(period: Interval): Graph[VD,ED] = {
    val index = intervals.indexOf(period)
    val filteredvas: RDD[(VertexId,VD)] = vertexattrs.filter{ case (k,v) => v._1 == index}
      .map{ case (k,v) => (k, v._2)}
    val filterededs: RDD[Edge[ED]] = edgeattrs.filter{ case (k,v) => v._1 == index}.map{ case (k,v) => Edge(k._1, k._2, v._2)}
    Graph[VD,ED](filteredvas, EdgeRDD.fromEdges[ED,VD](filterededs), 
      edgeStorageLevel = getStorageLevel,
      vertexStorageLevel = getStorageLevel)
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
      val vattrs = vertexattrs.filter{ case (k,v) => v._1 >= selectStart && v._1 < selectStop}.map{ case (k,v) => (k, (v._1 - selectStart, v._2))}
      val eattrs = edgeattrs.filter{ case (k,v) => v._1 >= selectStart && v._1 < selectStop}.map{ case (k,v) => (k, (v._1 - selectStart, v._2))}

      new OneGraphColumn[VD, ED](newIntvs, resg, vattrs, eattrs)

    } else
      OneGraphColumn.emptyGraph[VD,ED]()
  }

  override def select(tpred: Interval => Boolean): TemporalGraph[VD, ED] = {
    val chosen:ListBuffer[Int] = ListBuffer[Int]()
    intervals.zipWithIndex.foreach{ case (p,i) => if (tpred(p)) chosen += i} 
    //TODO: broadcast mask? it's prob too large
    val mask:BitSet = BitSet() ++ chosen
    val start = span.start
    val res = resolution

    new OneGraphColumn[VD, ED](intervals, graphs.subgraph(
      vpred = (vid, attr) => !(attr & mask).isEmpty,
      epred = et => !(et.attr & mask).isEmpty)
      .mapVertices((vid, vattr) => vattr & mask),
      vertexattrs.filter{ case (k,v) => tpred(res.getInterval(start, v._1))},
      edgeattrs.filter{ case (k,v) => tpred(res.getInterval(start, v._1))})
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
    var index:Int = 0
    //make a map of old indices to new ones
    //TODO: rewrite simpler. there is no reason for the cntMap to be a map
    val indMap:HashMap[TimeIndex, TimeIndex] = HashMap[TimeIndex, TimeIndex]()
    var counts:scala.collection.immutable.Seq[Int] = scala.collection.immutable.Seq[Int]()

    while (index < intervals.size) {
      val intv:Interval = intervals(index)
      //need to compute the interval start and end based on resolution new units
      val newIntv:Interval = res.getInterval(intv.start)
      val expected:Int = resolution.getNumParts(res, intv.start)

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

      counts = counts :+ expected
      intvs = intvs :+ newIntv
    }

    val broadcastIndMap = ProgramContext.sc.broadcast(indMap)
    val countSums = ProgramContext.sc.broadcast(counts.scanLeft(0)(_ + _).tail)

    //for each vertex, create a new bitset
    //then filter out those where bitset is all unset
    val filtered: Graph[BitSet, BitSet] = graphs.mapVertices { (vid, attr) =>
      BitSet() ++ (0 to (countSums.value.size-1)).flatMap{ case (index) =>
        //make a mask for this part
        val mask = BitSet((countSums.value.lift(index-1).getOrElse(0) to (countSums.value(index)-1)): _*)
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
      }}
      .subgraph(vpred = (vid, attr) => !attr.isEmpty)
      .mapEdges{ e =>
      BitSet() ++ (0 to (countSums.value.size-1)).flatMap{ case (index) =>
        //make a mask for this part
        val mask = BitSet((countSums.value.lift(index-1).getOrElse(0) to (countSums.value(index)-1)): _*)
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
      }}
      .subgraph(epred = et => !et.attr.isEmpty)

    //TODO: see if filtering can be done more efficiently
    val aveReductFactor = math.max(1, counts.head / 2)
    val vattrs = if (vsem == AggregateSemantics.All) vertexattrs.map{ case (k,v) => ((k, broadcastIndMap.value(v._1)), (v._2, 1))}.reduceByKey((x,y) => (vAggFunc(x._1, y._1), x._2 + y._2), math.max(4, vertexattrs.partitions.size / aveReductFactor)).filter{ case (k, (attr,cnt)) => cnt == counts(k._2)}.map{ case (k,v) => (k._1, (k._2, v._1))} else vertexattrs.map{ case (k,v) => ((k, broadcastIndMap.value(v._1)), v._2)}.reduceByKey(vAggFunc, math.max(4, vertexattrs.partitions.size / aveReductFactor)).map{ case (k,v) => (k._1, (k._2, v))}
    //FIXME: this does not filter out edges for which vertices no longer exist
    //such as would happen when vertex semantics is universal
    //and edges semantics existential
    val eattrs = if (esem == AggregateSemantics.All) 
      edgeattrs.map{ case (k,v) => ((k._1, k._2, broadcastIndMap.value(v._1)), (v._2, 1))}.reduceByKey((x,y) => (eAggFunc(x._1, y._1), x._2 + y._2), math.max(4, edgeattrs.partitions.size / aveReductFactor)).filter{ case (k, (attr,cnt)) => cnt == counts(k._3)}.map{ case (k,v) => ((k._1, k._2), (k._3, v._1))}
    else 
      edgeattrs.map{ case (k,v) => ((k._1, k._2, broadcastIndMap.value(v._1)), v._2)}.reduceByKey(eAggFunc, math.max(4, edgeattrs.partitions.size / aveReductFactor)).map{ case (k,v) => ((k._1, k._2), (k._3, v))}
    
    //FIXME: figure out when this can be destroyed if ever
    //broadcastIndMap.destroy()

    //TODO: it may be more efficient to coalesce to smaller number
    //of partitions here, especially for universal semantics
    new OneGraphColumn[VD, ED](intvs, filtered, vattrs, eattrs)
  }

  override def transform[ED2: ClassTag, VD2: ClassTag](emap: (Edge[ED], Interval) => ED2, vmap: (VertexId, Interval, VD) => VD2): TemporalGraph[VD2, ED2] = {
    val start = span.start
    val res = resolution
    new OneGraphColumn[VD2, ED2](intervals, graphs, vertexattrs.map{ case (k,v) => (k, (v._1, vmap(k, res.getInterval(start, v._1), v._2)))}, edgeattrs.map{ case (k,v) => (k, (v._1, emap(Edge(k._1, k._2, v._2), res.getInterval(start, v._1))))})
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val start = span.start
val res = resolution
    new OneGraphColumn[VD2, ED](intervals, graphs, vertexattrs.map{ case (k,v) => (k, (v._1, map(k, res.getInterval(start, v._1), v._2)))}, edgeattrs)
  }

  override def mapVerticesWIndex[VD2: ClassTag](map: (VertexId, TimeIndex, VD) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    new OneGraphColumn[VD2, ED](intervals, graphs, vertexattrs.map{ case (k,v) => (k, (v._1, map(k, v._1, v._2)))}, edgeattrs)
  }

  override def mapEdges[ED2: ClassTag](map: (Edge[ED], Interval) => ED2): TemporalGraph[VD, ED2] = {
    val start = span.start
    val res = resolution
    new OneGraphColumn[VD, ED2](intervals, graphs, vertexattrs, edgeattrs.map{ case (k,v) => (k, (v._1, map(Edge(k._1, k._2, v._2), res.getInterval(start, v._1))))})
  }

  override def mapEdgesWIndex[ED2: ClassTag](map: (Edge[ED], TimeIndex) => ED2): TemporalGraph[VD, ED2] = {
    new OneGraphColumn[VD, ED2](intervals, graphs, vertexattrs, edgeattrs.map{ case (k,v) => (k, (v._1, map(Edge(k._1, k._2, v._2), v._1)))})
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, Map[Interval, U])])(mapFunc: (VertexId, Interval, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val start = span.start
    val res = resolution

    val in: RDD[((VertexId,TimeIndex),U)] = other.flatMap(x => x._2.map(y => ((x._1, res.numBetween(start, y._1.start)), y._2)))

    new OneGraphColumn[VD2, ED](intervals, graphs, vertexattrs.map{ case (k,v) => ((k, v._1), v._2)}.leftOuterJoin(in).map{ case (k,v) => (k._1, (k._2, mapFunc(k._1, res.getInterval(start, k._2), v._1, v._2)))}, edgeattrs)
  }

  override def union(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    /** The type checking already validates that the structurally the graphs are union-compatible
      * But we also need to check that they are temporally union-compatible
      * this includes having the same resolution and aligning intervals
      * Two temporal sequences are compatible if their first elements are compatible
      */ 
    var grp2: OneGraphColumn[VD, ED] = other match {
      case grph: OneGraphColumn[VD, ED] => grph
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
    val gr1Verts = if (gr1IndexStart > 0) graphs.vertices.mapValues{ (vid:VertexId,vattr:BitSet) => vattr.map(_ + gr1IndexStart)} else graphs.vertices
    val gr2Verts = if (gr2IndexStart > 0) grp2.graphs.vertices.mapValues{ (vid:VertexId, vattr:BitSet) => vattr.map(_ + gr2IndexStart)} else grp2.graphs.vertices
    val gr1Edges = if (gr1IndexStart > 0) graphs.edges.mapValues{ e => e.attr.map(_ + gr1IndexStart)} else graphs.edges
    val gr2Edges = if (gr2IndexStart > 0) grp2.graphs.edges.mapValues{ e => e.attr.map(_ + gr2IndexStart)} else grp2.graphs.edges

    //we want to keep those vertices that exist either in either or both graphs
    //depending on the semantics
    val newverts = if (sem == AggregateSemantics.All) (gr1Verts union gr2Verts).reduceByKey{ (a: BitSet, b: BitSet) => a & b }.filter{ case (vid, vattr) => !vattr.isEmpty} else (gr1Verts union gr2Verts).reduceByKey{ (a: BitSet, b: BitSet) => a.union(b)  }
    val newedges = if (sem == AggregateSemantics.All) (gr1Edges union gr2Edges).map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey{ (a: BitSet, b: BitSet) => a & b }.filter{ x => !x._2.isEmpty}.map{ case (k,v) => Edge(k._1, k._2, v)} else (gr1Edges union gr2Edges).map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey{ (a: BitSet, b: BitSet) => a.union(b) }.map{ case (k,v) => Edge(k._1, k._2, v)}

    val gr1vattrs = vertexattrs.map{ case (k,v) => ((k, v._1 + gr1IndexStart), v._2)}
    val gr2vattrs = grp2.vertexattrs.map{ case (k,v) => ((k, v._1 + gr2IndexStart), v._2)}
    //now put them together
    val vattrs = if (sem == AggregateSemantics.All) (gr1vattrs join gr2vattrs).map{ case (k,v) => (k._1, (k._2, vFunc(v._1, v._2)))} else (gr1vattrs union gr2vattrs).reduceByKey(vFunc).map{ case (k,v) => (k._1, (k._2, v))}
    val gr1eattrs = edgeattrs.map{ case (k,v) => ((k._1, k._2, v._1 + gr1IndexStart), v._2)}
    val gr2eattrs = grp2.edgeattrs.map{ case (k,v) => ((k._1, k._2, v._1 + gr2IndexStart), v._2)}
    val eattrs = if (sem == AggregateSemantics.All) (gr1eattrs join gr2eattrs).map{ case (k,v) => ((k._1, k._2), (k._3, eFunc(v._1, v._2)))} else (gr1eattrs union gr2eattrs).reduceByKey(eFunc).map{ case (k,v) => ((k._1, k._2), (k._3, v))}

    new OneGraphColumn[VD, ED](mergedIntervals, Graph[BitSet,BitSet](newverts, EdgeRDD.fromEdges[BitSet,BitSet](newedges), edgeStorageLevel = getStorageLevel, vertexStorageLevel = getStorageLevel), vattrs, eattrs)
  }

  override def intersect(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    /** The type checking already validates that the structurally the graphs are union-compatible
      * But we also need to check that they are temporally union-compatible
      * this includes having the same resolution and aligning intervals
      * Two temporal sequences are compatible if their first elements are compatible
      */ 
    var grp2: OneGraphColumn[VD, ED] = other match {
      case grph: OneGraphColumn[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (!intervals.head.isUnionCompatible(grp2.intervals.head)) {
      throw new IllegalArgumentException("two graphs are not union-compatible in the temporal schema")
    }

    //compute the combined span
    val startBound = if (span.start.isBefore(grp2.span.start)) grp2.span.start else span.start
    val endBound = if (span.end.isAfter(grp2.span.end)) grp2.span.end else span.end
    if (startBound.isAfter(endBound) || startBound.isEqual(endBound)) {
      OneGraphColumn.emptyGraph[VD,ED]()
    } else {
      //we are taking a temporal subset of both graphs
      //and then doing the structural part
      val gr1Sel = select(Interval(startBound, endBound))  match {
        case grph: OneGraphColumn[VD, ED] => grph
        case _ => throw new ClassCastException
      }
      val gr2Sel = grp2.select(Interval(startBound, endBound)) match {
        case grph: OneGraphColumn[VD, ED] => grph
        case _ => throw new ClassCastException
      }

      //now union
      //we want to keep those vertices that exist either in either or both graphs
      //depending on the semantics
      val newverts = if (sem == AggregateSemantics.All) (gr1Sel.graphs.vertices union gr2Sel.graphs.vertices).reduceByKey{ (a: BitSet, b: BitSet) => a & b }.filter{ case (vid, vattr) => !vattr.isEmpty} else (gr1Sel.graphs.vertices union gr2Sel.graphs.vertices).reduceByKey{ (a: BitSet, b: BitSet) => a.union(b)  }
      val newedges = if (sem == AggregateSemantics.All) (gr1Sel.graphs.edges union gr2Sel.graphs.edges).map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey{ (a: BitSet, b: BitSet) => a & b }.filter{ x => !x._2.isEmpty}.map{ case (k,v) => Edge(k._1, k._2, v)} else (gr1Sel.graphs.edges union gr2Sel.graphs.edges).map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey{ (a: BitSet, b: BitSet) => a.union(b) }.map{ case (k,v) => Edge(k._1, k._2, v)}

      val vattrs = if (sem == AggregateSemantics.All) (gr1Sel.vertexattrs.map{ case (k,v) => ((k, v._1), v._2)} join gr2Sel.vertexattrs.map{ case (k,v) => ((k, v._1), v._2)}).map{ case (k,v) => (k._1, (k._2, vFunc(v._1, v._2)))} else (gr1Sel.vertexattrs.map{ case (k,v) => ((k, v._1), v._2)} union gr2Sel.vertexattrs.map{ case (k,v) => ((k, v._1), v._2)}).reduceByKey(vFunc).map{ case (k,v) => (k._1, (k._2, v))}
      val eattrs = if (sem == AggregateSemantics.All) (gr1Sel.edgeattrs.map{ case (k,v) => ((k._1, k._2, v._1), v._2)} join gr2Sel.edgeattrs.map{ case (k,v) => ((k._1, k._2, v._1), v._2)}).map{ case (k,v) => ((k._1, k._2), (k._3, eFunc(v._1, v._2)))} else (gr1Sel.edgeattrs.map{ case (k,v) => ((k._1, k._2, v._1), v._2)} union gr2Sel.edgeattrs.map{ case (k,v) => ((k._1, k._2, v._1), v._2)}).reduceByKey(eFunc).map{case (k,v) => ((k._1, k._2), (k._3, v))}

      new OneGraphColumn[VD, ED](gr2Sel.intervals, Graph[BitSet,BitSet](newverts, EdgeRDD.fromEdges[BitSet,BitSet](newedges),
        edgeStorageLevel = getStorageLevel,
        vertexStorageLevel = getStorageLevel), vattrs, eattrs)
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

    //need to put values into vertices and edges
    //FIXME: is this really necessary? they aren't used in the computations!
    val grph = Graph[Map[TimeIndex,VD], Map[TimeIndex,ED]](
      vertexattrs.mapValues{ v => Map[TimeIndex, VD](v._1 -> v._2)}.reduceByKey((a, b) => a ++ b),
      EdgeRDD.fromEdges[Map[TimeIndex,ED],Map[TimeIndex,VD]](edgeattrs.mapValues{ v => Map(v._1 -> v._2)}.reduceByKey((a,b) => a ++ b).map{ case (k,v) => Edge(k._1, k._2, v)}),
      edgeStorageLevel = getStorageLevel,
      vertexStorageLevel = getStorageLevel)
      
    val newgrp: Graph[Map[TimeIndex, VD], Map[TimeIndex, ED]] = Pregel(grph, initM, maxIterations, activeDirection)(vertexP, sendMsgC, mergeMsgC)
    //need to convert back to bitmap and vertexattrs
    //FIXME:? is it ok that we are throwing away the new edge values?
    val newattrs: RDD[(VertexId, (TimeIndex, VD))] = newgrp.vertices.flatMap{ case (vid, vattr) => vattr.toSeq.map{ case (k,v) => (vid, (k,v))  }}

    new OneGraphColumn[VD, ED](intervals, graphs, newattrs, edgeattrs)
  }

  //run pagerank on each interval
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): TemporalGraph[Double, Double] = {
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
      val vattrs = resultGraph.vertices.flatMap{ case (vid,vattr) => vattr.toSeq.map{ case (k,v) => (vid,(k, v._1))}}
      val eattrs = resultGraph.edges.flatMap{ e => e.attr.toSeq.map{ case (k,v) => ((e.srcId, e.dstId), (k, v._1))}}

      new OneGraphColumn[Double, Double](intervals, graphs, vattrs, eattrs)

    } else {
      //TODO: implement this using pregel
      throw new UnsupportedOperationException("directed version of pageRank not yet implemented")
    }
  }
  
/*
  override def degree(): TemporalGraph[Double, Double] = {
      def mergeFunc(a:Map[TimeIndex,Int], b:Map[TimeIndex,Int]): Map[TimeIndex,Int] = {
        a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
      }

      val degrees: VertexRDD[Map[TimeIndex,Int]] = graphs.aggregateMessages[Map[TimeIndex, Int]](
        ctx => {
          ctx.sendToSrc(ctx.attr.seq.map(x => (x,1)).toMap)
          ctx.sendToDst(ctx.attr.seq.map(x => (x,1)).toMap)
        },
        mergeFunc, TripletFields.None)


    val vattrs = degrees.flatMap{ case (vid, vattr) => vattr.toSeq.map{ case (k,v) => (vid, (k, v.toDouble))}}
    val eattrs = edgeattrs.map{ case (k, attr) => (k, (attr._1, 0.0))}

    new OneGraphColumn[Double, Double](intervals, graphs, vattrs, eattrs)
  }
 */

  //run connected components on each interval
  override def connectedComponents(): TemporalGraph[VertexId, ED] = {
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

    val vattrs = resultGraph.vertices.flatMap{ case (vid, vattr) => vattr.toSeq.map{ case (k,v) => (vid, (k, v))}}

    new OneGraphColumn[VertexId, ED](intervals, graphs, vattrs, edgeattrs)
  }
  
  //run shortestPaths on each interval
  override def shortestPaths(landmarks: Seq[VertexId]): TemporalGraph[ShortestPathsXT.SPMap, ED] = {
    //TODO: implement this using pregel
    throw new UnsupportedOperationException("directed version of shortestpaths not yet implemented")
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    if (graphs.edges.isEmpty)
      0
    else
      graphs.edges.partitions.size
  }

  def getStorageLevel: StorageLevel = graphs.edges.getStorageLevel

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): TemporalGraph[VD, ED] = {
    graphs.persist(newLevel)
    vertexattrs.persist(newLevel)
    edgeattrs.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): TemporalGraph[VD, ED] = {
    graphs.unpersist(blocking)
    vertexattrs.unpersist(blocking)
    edgeattrs.unpersist(blocking)
    this
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int): TemporalGraph[VD, ED] = {
    partitionBy(pst, runs, graphs.edges.partitions.size)
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): TemporalGraph[VD, ED] = {
    var numParts = if (parts > 0) parts else graphs.edges.partitions.size

    if (pst != PartitionStrategyType.None) {
      //not changing the intervals
      new OneGraphColumn[VD, ED](intervals, graphs.partitionByExt(PartitionStrategies.makeStrategy(pst, 0, intervals.size, runs), numParts), vertexattrs, edgeattrs)
    } else
      this
  }
}

object OneGraphColumn {
  final def loadData(dataPath: String, start: LocalDate, end: LocalDate): OneGraphColumn[String, Int] = {
    loadWithPartition(dataPath, start, end, PartitionStrategyType.None, 1)
  }

  final def loadWithPartition(dataPath: String, start: LocalDate, end: LocalDate, strategy: PartitionStrategyType.Value, runWidth: Int): OneGraphColumn[String, Int] = {
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

    val usersnp: RDD[(VertexId,(TimeIndex,String))] = MultifileLoad.readNodes(dataPath, minDate, intvs.last.start).flatMap{ x => 
      val (filename, line) = x
      val dt = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))
      val parts = line.split(",")
      val index = res.numBetween(minDate, dt)
      if (parts.size > 1 && parts.head != "" && index > -1) {
        Some(parts.head.toLong, (index, parts(1).toString))
      } else None
    }

    val users = usersnp.partitionBy(new HashPartitioner(usersnp.partitions.size))

    val linksnp: RDD[((VertexId,VertexId),(TimeIndex,Int))] = MultifileLoad.readEdges(dataPath, minDate, intvs.last.start)
      .flatMap{ x =>
      val (filename, line) = x
      val dt = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))
      if (!line.isEmpty && line(0) != '#') {
        val lineArray = line.split("\\s+")
        val srcId = lineArray(0).toLong
        val dstId = lineArray(1).toLong
        var attr = 0
        if(lineArray.length > 2){
          attr = lineArray(2).toInt
        }
        val index = res.numBetween(minDate, dt)
        if (srcId > dstId)
          Some((dstId, srcId), (index,attr))
        else
          Some((srcId, dstId), (index,attr))
      } else None
    }
    val links = linksnp.partitionBy(new HashPartitioner(linksnp.partitions.size))

    //TODO: make these hard-coded parameters be dependent on  data size
    //and evolution rate
    val reductFact: Int = 2
    val verts: RDD[(VertexId, BitSet)] = users.mapValues{ v => BitSet(v._1)}.reduceByKey((a,b) => a union b, math.max(4, users.partitions.size/reductFact) )
    val edges = EdgeRDD.fromEdges[BitSet, BitSet](links.mapValues{ v => BitSet(v._1)}.reduceByKey((a,b) => a union b, math.max(4, links.partitions.size/reductFact)).map{case (k,v) => Edge(k._1, k._2, v)})

    //TODO: the storage level should depend on size becase MEMORY_ONLY is fine for small graphs
    var graph: Graph[BitSet,BitSet] = Graph(verts, edges, BitSet(), edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)

    if (strategy != PartitionStrategyType.None) {
      graph = graph.partitionBy(PartitionStrategies.makeStrategy(strategy, 0, intvs.size, runWidth))
    }    

    /*
     val degs:RDD[Double] = graph.degrees.map{ case (vid,attr) => attr}
     println("min degree: " + degs.min)
     println("max degree: " + degs.max)
     println("average degree: " + degs.mean)
     val counts = degs.histogram(Array(0.0, 10, 50, 100, 1000, 5000, 10000, 50000, 100000, 250000))
     println("histogram:" + counts.mkString(","))
     println("number of vertices: " + graph.vertices.count)
     println("number of edges: " + graph.edges.count)
     println("number of partitions in edges: " + graph.edges.partitions.size)
    */

    new OneGraphColumn[String, Int](intvs, graph, users, links)
  }

  def emptyGraph[VD: ClassTag, ED: ClassTag]():OneGraphColumn[VD, ED] = new OneGraphColumn(Seq[Interval](), Graph[BitSet,BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD), ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)

}
