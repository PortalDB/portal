//One multigraph
//Each vertex and edge has a value attribute associated with each time period
package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.parallel.ParSeq
import scala.collection.mutable.HashMap
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

class MultiGraph[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], grs: Graph[Map[TimeIndex, VD], (TimeIndex, ED)]) extends TemporalGraph[VD, ED] with Serializable {
  val graphs: Graph[Map[TimeIndex, VD], (TimeIndex, ED)] = grs
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
  protected def this() = this(Seq[Interval](), Graph[Map[TimeIndex, VD], (TimeIndex, ED)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD))

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

    //compute degree of each vertex for each interval
    //this should produce a map from interval to degree for each vertex
    //since edge is treated by MultiGraph as undirectional, can use the edge markings
    val start = span.start
    graphs.aggregateMessages[Map[TimeIndex,Int]](
      ctx => {
        ctx.sendToSrc(Map(ctx.attr._1 -> 1))
        ctx.sendToDst(Map(ctx.attr._1 -> 1))
      },
      mergedFunc,
      TripletFields.All)
    .mapValues(v => v.map(x => (resolution.getInterval(start, x._1) -> x._2)))
  }

  override def getTemporalSequence: Seq[Interval] = intervals

  override def getSnapshot(period: Interval): Graph[VD,ED] = {
    val index = intervals.indexOf(period)
    graphs.subgraph(
      vpred = (vid, attr) => !attr.filter{ case (k,v) => k == index}.isEmpty,
      epred = et => et.attr._1 == index)
      .mapVertices((vid,vattr) => vattr.filter{ case (k,v) => k == index}.head._2)
      .mapEdges(e => e.attr._2)
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
        epred = et => et.attr._1 >= selectStart && et.attr._1 < selectStop)

      //now need to renumber vertex and edge intervals indices
      //indices outside of selected range get dropped
      val resg = subg.mapVertices((vid,vattr) => vattr.filter{case (k,v) => k >= selectStart && k < selectStop}.map(x => (x._1 - selectStart -> x._2))).mapEdges(e => (e.attr._1 - selectStart, e.attr._2))

      new MultiGraph[VD, ED](newIntvs, resg)

    } else
      MultiGraph.emptyGraph[VD,ED]()
  }

  override def select(tpred: Interval => Boolean): TemporalGraph[VD, ED] = {
    val start = span.start
    new MultiGraph[VD, ED](intervals, graphs.subgraph(
      vpred = (vid, attr) => !attr.filter{ case (k,v) => tpred(resolution.getInterval(start, k))}.isEmpty,
      epred = et => tpred(resolution.getInterval(start, et.attr._1))))
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
    val wverts = graphs.mapVertices { (vid, attr) =>
      var tmp: Map[Int, Seq[VD]] = attr.toSeq.map { case (k, v) => (indMap(k), v) }.groupBy { case (k, v) => k }.mapValues { v => v.map { case (x, y) => y } }
      //tmp is now a map of (index, list(attr))
      if (sem == AggregateSemantics.Universal) {
        tmp = tmp.filter { case (k, v) => v.size == cntMap(k) }
      }
      tmp.mapValues { v => v.reduce(vAggFunc) }.map(identity)
    }.subgraph(vpred = (vid, attr) => !attr.isEmpty, epred = e => true).mapEdges { e => (indMap(e.attr._1), e.attr._2)}

    var edges: EdgeRDD[(TimeIndex, ED)] = null
    if (sem == AggregateSemantics.Existential) {
      edges = EdgeRDD.fromEdges[(TimeIndex, ED), VD](wverts.edges.map(x => ((x.srcId, x.dstId, indMap(x.attr._1)), x.attr._2)).reduceByKey(eAggFunc).map { x =>
        val (k, v) = x
        //key is srcid, dstid, resolution, value is attrib
        Edge(k._1, k._2, (k._3, v))
      })
    } else if (sem == AggregateSemantics.Universal) {
      edges = EdgeRDD.fromEdges[(TimeIndex, ED), VD](wverts.edges.map(x => ((x.srcId, x.dstId, indMap(x.attr._1)), (x.attr._2, 1))).reduceByKey((x, y) => (eAggFunc(x._1, y._1), x._2 + y._2)).filter { case (k, (attr, cnt)) => cnt == cntMap(k._3) }.map { x =>
        val (k, v) = x
        //key is srcid, dstid, resolution, value is attrib and count
        Edge(k._1, k._2, (k._3, v._1))
      })
    }

    new MultiGraph[VD, ED](intvs, Graph(wverts.vertices, edges))
  }

  override def transform[ED2: ClassTag, VD2: ClassTag](emap: (Edge[ED], Interval) => ED2, vmap: (VertexId, Interval, VD) => VD2): TemporalGraph[VD2, ED2] = {
    val start = span.start
    new MultiGraph[VD2, ED2](intervals, graphs.mapVertices{ (id: VertexId, attr: Map[TimeIndex, VD]) => attr.map(x => (x._1, vmap(id, resolution.getInterval(start, x._1), x._2)))}
      .mapEdges{ e: Edge[(TimeIndex, ED)] => (e.attr._1, emap(Edge(e.srcId, e.dstId, e.attr._2), resolution.getInterval(start, e.attr._1)))})
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val start = span.start
    new MultiGraph[VD2, ED](intervals, graphs.mapVertices{ (id: VertexId, attr: Map[TimeIndex, VD]) =>
      attr.map(x => (x._1, map(id, resolution.getInterval(start, x._1), x._2)))
    }
    )
  }

  override def mapEdges[ED2: ClassTag](map: (Edge[ED], Interval) => ED2): TemporalGraph[VD, ED2] = {
    val start = span.start
    new MultiGraph[VD, ED2](intervals, graphs.mapEdges{ e: Edge[(TimeIndex, ED)] =>
      (e.attr._1, map(Edge(e.srcId, e.dstId, e.attr._2), resolution.getInterval(start, e.attr._1)))
    }
    )
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, Map[Interval, U])])(mapFunc: (VertexId, Interval, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    //convert the intervals to corresponding indices
    val in:RDD[(VertexId, Map[TimeIndex, U])] = other.mapValues { attr: Map[Interval, U] =>
      attr.filterKeys(k => span.contains(k)).map{ case (intvl, attr) => (intervals.indexOf(intvl), attr)}
    }
    val start = span.start
    new MultiGraph[VD2, ED](intervals, graphs.outerJoinVertices(in){ (id: VertexId, attr1: Map[TimeIndex, VD], attr2: Option[Map[TimeIndex, U]]) =>
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
    var grp2: MultiGraph[VD, ED] = other match {
      case grph: MultiGraph[VD, ED] => grph
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
    val gr1Verts = if (gr1IndexStart > 0) graphs.vertices.mapValues{ (vid:VertexId,vattr:Map[TimeIndex,VD]) => vattr.map{ case (k,v) => (k + gr1IndexStart, v)} } else graphs.vertices
    val gr2Verts = if (gr2IndexStart > 0) grp2.graphs.vertices.mapValues{ (vid:VertexId, vattr:Map[TimeIndex,VD]) => vattr.map{ case (k,v) => (k+gr2IndexStart, v)} } else grp2.graphs.vertices

    //now union
    var target = if (sem == AggregateSemantics.Universal) 2 else 1
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

    //now similar with edges, except that we allow multiple edges
    //between the same pair of vertices if the time period is different
    //so the reduce is more complicated
    val newedges = (gr1Edges union gr2Edges).map(x => ((x.srcId, x.dstId, x.attr._1), (x.attr._2, 1)))
      .reduceByKey((e1,e2) => (eFunc(e1._1, e2._1), e1._2 + e2._2))
      .filter{ case (k, (attr, cnt)) => cnt >= target}
      .map{ case (k,v) => Edge(k._1, k._2, (k._3, v._1))}

    new MultiGraph[VD, ED](mergedIntervals, Graph[Map[TimeIndex, VD], (TimeIndex, ED)](newverts, EdgeRDD.fromEdges[(TimeIndex, ED),Map[TimeIndex,VD]](newedges)))
  }

  override def intersect(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    /** The type checking already validates that the structurally the graphs are union-compatible
      * But we also need to check that they are temporally union-compatible
      * this includes having the same resolution and aligning intervals
      * Two temporal sequences are compatible if their first elements are compatible
      */ 
    var grp2: MultiGraph[VD, ED] = other match {
      case grph: MultiGraph[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (!intervals.head.isUnionCompatible(grp2.intervals.head)) {
      throw new IllegalArgumentException("two graphs are not union-compatible in the temporal schema")
    }

    //compute the combined span
    val startBound = if (span.start.isBefore(grp2.span.start)) grp2.span.start else span.start
    val endBound = if (span.end.isAfter(grp2.span.end)) grp2.span.end else span.end
    if (startBound.isAfter(endBound) || startBound.isEqual(endBound)) {
      MultiGraph.emptyGraph[VD,ED]()
    } else {
      //we are taking a temporal subset of both graphs
      //and then doing the structural part
      val gr1Sel = select(Interval(startBound, endBound))  match {
        case grph: MultiGraph[VD, ED] => grph
        case _ => throw new ClassCastException
      }
      val gr2Sel = grp2.select(Interval(startBound, endBound)) match {
        case grph: MultiGraph[VD, ED] => grph
        case _ => throw new ClassCastException
      }

      //now union
      var target = if (sem == AggregateSemantics.Universal) 2 else 1
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

      //now similar with edges, except that we allow multiple edges
      //between the same pair of vertices if the time period is different
      //so the reduce is more complicated
      val newedges = (gr1Sel.graphs.edges union gr2Sel.graphs.edges).map(x => ((x.srcId, x.dstId, x.attr._1), (x.attr._2, 1)))
        .reduceByKey((e1,e2) => (eFunc(e1._1, e2._1), e1._2 + e2._2))
        .filter{ case (k, (attr, cnt)) => cnt >= target}
        .map{ case (k,v) => Edge(k._1, k._2, (k._3, v._1))}

      new MultiGraph[VD, ED](gr2Sel.intervals, Graph[Map[TimeIndex, VD], (TimeIndex, ED)](newverts, EdgeRDD.fromEdges[(TimeIndex,ED),Map[TimeIndex,VD]](newedges)))
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

    val newgrp: Graph[Map[TimeIndex, VD], (TimeIndex, ED)] = Pregel(graphs, initM, maxIterations, activeDirection)(vertexP, sendMsgC, mergeMsgC)
    new MultiGraph[VD, ED](intervals, newgrp)
  }

  //run pagerank on each interval
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): TemporalGraph[Double, Double] = {
    if (uni)
      new MultiGraph[Double, Double](intervals, UndirectedPageRank.runCombined(graphs, intervals.size, tol, resetProb, numIter))
    else
      //TODO: implement this using pregel
      throw new UnsupportedOperationException("directed version of pageRank not yet implemented")
  }
  
  //run connected components on each interval
  override def connectedComponents(): TemporalGraph[VertexId, ED] = {
      new MultiGraph[VertexId, ED](intervals, ConnectedComponentsXT.runCombined(graphs, intervals.size))
  }
  
  //run shortestPaths on each interval
  override def shortestPaths(landmarks: Seq[VertexId]): TemporalGraph[ShortestPathsXT.SPMap, ED] = {
      new MultiGraph[ShortestPathsXT.SPMap, ED](intervals, ShortestPathsXT.runCombined(graphs, landmarks, intervals.size))
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
      new MultiGraph[VD, ED](intervals, graphs.partitionByExt(PartitionStrategies.makeStrategy(pst, 0, intervals.size, runs), numParts))
    } else
      this
  }

}

object MultiGraph {
  final def loadData(dataPath: String, start: LocalDate, end: LocalDate): MultiGraph[String, Int] = {
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
    val edges = GraphLoaderAddon.edgeListFiles(in, res.period, res.unit, minDate, true).edges

    val graph: Graph[Map[Int, String], (Int, Int)] = Graph(users, edges, Map[Int,String]())

    edges.unpersist()

    new MultiGraph[String, Int](intvs, graph.persist())
  }

  final def loadWithPartition(dataPath: String, start: LocalDate, end: LocalDate, strategy: PartitionStrategyType.Value, runWidth: Int): MultiGraph[String, Int] = {
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
    var edges = GraphLoaderAddon.edgeListFiles(in, res.period, res.unit, minDate, true)

    if (strategy != PartitionStrategyType.None) {
      val numParts = edges.edges.partitions.size
      edges = edges.partitionByExt(PartitionStrategies.makeStrategy(strategy, 0, intvs.size, runWidth),numParts)
    }

    val graph: Graph[Map[Int, String], (Int, Int)] = Graph(users, edges.edges, Map[Int,String]())

    edges.unpersist()

    new MultiGraph[String, Int](intvs, graph.persist())
  }

  def emptyGraph[VD: ClassTag, ED: ClassTag]():MultiGraph[VD, ED] = new MultiGraph(Seq[Interval](), Graph[Map[TimeIndex, VD],(TimeIndex, ED)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD))

}
