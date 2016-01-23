package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.mutable.Buffer
import scala.reflect.ClassTag
import scala.util.control._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import org.apache.spark.SparkContext
import org.apache.spark.Partition

import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphLoaderAddon
import org.apache.spark.rdd._
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import edu.drexel.cs.dbgroup.temporalgraph._

import java.time.LocalDate

class SnapshotGraph[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], grs: Seq[Graph[VD, ED]]) extends TemporalGraph[VD, ED] {
  val graphs: Seq[Graph[VD, ED]] = grs
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
  protected def this() = this(Seq[Interval](), Seq[Graph[VD, ED]]())

  override def size(): Int = { graphs.size }

  override def materialize() = {
    //just call count on all vertices and edges
    graphs.foreach { x =>
      x.numEdges
      x.numVertices
    }
  }

  override def vertices: VertexRDD[Map[Interval, VD]] = {
    if (size > 0) {
      val total = graphs.zipWithIndex.filterNot(x => x._1.vertices.isEmpty).map(x => x._1.vertices.mapValues(y => Map[Interval, VD](intervals(x._2) -> y)))
      if (total.size > 0)
        VertexRDD(total.reduce((a, b) => VertexRDD(a union b))
          .reduceByKey((a: Map[Interval, VD], b: Map[Interval, VD]) => a ++ b))
      else {
        val ret:VertexRDD[Map[Interval,VD]] = VertexRDD(ProgramContext.sc.emptyRDD)
        ret
      }
    } else {
      val ret:VertexRDD[Map[Interval,VD]] = VertexRDD(ProgramContext.sc.emptyRDD)
      ret
    }
  }

  override def verticesFlat: VertexRDD[(Interval, VD)] = {
    if (size > 0) {
      val total = graphs.zipWithIndex.filterNot(x => x._1.vertices.isEmpty)
      if (total.size > 0)
        VertexRDD(total.map(x => x._1.vertices.mapValues(y => (intervals(x._2), y)))
          .reduce((a, b) => VertexRDD(a union b)))
      else {
        val ret:VertexRDD[(Interval,VD)] = VertexRDD(ProgramContext.sc.emptyRDD)
        ret
      }
    } else {
      val ret:VertexRDD[(Interval,VD)] = VertexRDD(ProgramContext.sc.emptyRDD)
      ret
    }
  }

  override def edges: EdgeRDD[Map[Interval, ED]] = {
    if (size > 0) {
      val total = graphs.zipWithIndex.filterNot(x => x._1.edges.isEmpty)
      if (total.size > 0)
        EdgeRDD.fromEdges[Map[Interval, ED], VD](total
          .map(x => x._1.edges.mapValues(y => Map[Interval, ED](intervals(x._2) -> y.attr)))
          .reduce((a: RDD[Edge[Map[Interval, ED]]], b: RDD[Edge[Map[Interval, ED]]]) => a union b)
          .map(x => ((x.srcId, x.dstId), x.attr))
          .reduceByKey((a: Map[Interval, ED], b: Map[Interval, ED]) => a ++ b)
          .map(x => Edge(x._1._1, x._1._2, x._2))
        )
      else
        EdgeRDD.fromEdges[Map[Interval, ED], VD](ProgramContext.sc.emptyRDD)
    } else {
      EdgeRDD.fromEdges[Map[Interval, ED], VD](ProgramContext.sc.emptyRDD)
    }
  }

  override def edgesFlat: EdgeRDD[(Interval, ED)] = {
    if (size > 0) {
      val total = graphs.zipWithIndex.filterNot(x => x._1.edges.isEmpty)
      if (total.size > 0)
        EdgeRDD.fromEdges[(Interval, ED), VD](total.map(x => x._1.edges.mapValues(y => (intervals(x._2),y.attr)))
          .reduce((a: RDD[Edge[(Interval, ED)]], b: RDD[Edge[(Interval, ED)]]) => a union b))
      else
        EdgeRDD.fromEdges[(Interval, ED), VD](ProgramContext.sc.emptyRDD)
    } else
      EdgeRDD.fromEdges[(Interval, ED), VD](ProgramContext.sc.emptyRDD)
  }

  override def degrees: VertexRDD[Map[Interval, Int]] = {
    if (size > 0) {
      val total = graphs.zipWithIndex.filterNot(x => x._1.edges.isEmpty).map(x => x._1.degrees.mapValues(deg => Map[Interval, Int](intervals(x._2) -> deg)))
      if (total.size > 0)
        VertexRDD(total.reduce((x,y) => VertexRDD(x union y))
          .reduceByKey((a: Map[Interval, Int], b: Map[Interval, Int]) => a ++ b))
      else {
        val ret:VertexRDD[Map[Interval, Int]] = VertexRDD(ProgramContext.sc.emptyRDD)
        ret
      }
    } else {
      val ret:VertexRDD[Map[Interval, Int]] = VertexRDD(ProgramContext.sc.emptyRDD)
      ret
    }
  }

  override def getTemporalSequence: Seq[Interval] = intervals

  override def getSnapshot(period: Interval): Graph[VD,ED] = {
    val index = intervals.indexOf(period)
    if (index >= 0) {
      graphs(index)
    } else
      Graph[VD,ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
  }

  /** Query operations */

  override def select(bound: Interval): TemporalGraph[VD, ED] = {
    if (span.intersects(bound)) {
      //need to find the start of the first interval at/after bound.start
      //and last interval at/before bound.end
      val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
      val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end

      var intvs: Seq[Interval] = Seq[Interval]()
      var gps: Seq[Graph[VD, ED]] = Seq[Graph[VD, ED]]()

      var numResults = 0
      val loop = new Breaks

      loop.breakable {
        intervals.zipWithIndex.foreach {
          case (k,v) =>
            if (numResults == 0 && k.contains(startBound)) {
              intvs = intvs :+ k
              numResults = 1
              gps = gps :+ graphs(v)
            } else if (numResults > 0 && k.contains(endBound)) {
              loop.break
            } else if (numResults > 0) {
              intvs = intvs :+ k
              numResults += 1
              gps = gps :+ graphs(v)
            }
        }
      }

      if (numResults == 0)
        SnapshotGraph.emptyGraph[VD,ED]()
      else
        new SnapshotGraph(intvs, gps)
    } else
      SnapshotGraph.emptyGraph[VD,ED]()
  }

  override def select(tpred: Interval => Boolean): TemporalGraph[VD, ED] = {
    if (size > 0) {
      var gps: Seq[Graph[VD, ED]] = Seq[Graph[VD, ED]]()
      intervals.zipWithIndex.foreach { case (k,v) =>
        if (tpred(k))
          gps = gps :+ graphs(v)
        else
          gps = gps :+ Graph[VD, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      }
      new SnapshotGraph(intervals, gps)
    } else SnapshotGraph.emptyGraph[VD,ED]()
  }

  override def select(epred: EdgeTriplet[VD,ED] => Boolean, vpred: (VertexId, VD) => Boolean): TemporalGraph[VD, ED] = {
    if (size > 0) new SnapshotGraph(intervals, graphs.map(x => x.subgraph(epred, vpred))) else SnapshotGraph.emptyGraph[VD,ED]()
  }

  override def aggregate(res: Resolution, vsem: AggregateSemantics.Value, esem: AggregateSemantics.Value, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    if (size == 0)
      return SnapshotGraph.emptyGraph[VD,ED]()

    var intvs: Seq[Interval] = Seq[Interval]()
    var gps: Seq[Graph[VD, ED]] = Seq[Graph[VD, ED]]()

    if (!resolution.isCompatible(res)) {
      throw new IllegalArgumentException("incompatible resolution")
    }

    //it is possible that different number of graphs end up in different intervals
    //such as going from days to months
    var index:Integer = 0
    var num:Integer = 0

    while (index < graphs.size) {
      val intv:Interval = intervals(index)
      num = 0
      //need to compute the interval start and end based on resolution new units
      val newIntv:Interval = res.getInterval(intv.start)
      val expected:Integer = resolution.getNumParts(res, intv.start)

      var firstVRDD: RDD[(VertexId, VD)] = graphs(index).vertices
      var firstERDD: RDD[Edge[ED]] = graphs(index).edges
      index += 1
      num += 1

      //grab all the intervals that fit within
      val loop = new Breaks
      loop.breakable {
        while (index < graphs.size) {
          val intv2:Interval = intervals(index)
          if (newIntv.contains(intv2)) {
            firstVRDD = firstVRDD.union(graphs(index).vertices)
            firstERDD = firstERDD.union(graphs(index).edges)
          } else {
            loop.break
          }
          num += 1
          index += 1
        }
      }

      intvs = intvs :+ newIntv
      //separate semantics for vertex and edge aggregation
      val vrdd = if (vsem == AggregateSemantics.Any) VertexRDD(firstVRDD.reduceByKey(vAggFunc)) else VertexRDD(firstVRDD.map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (vAggFunc(x._1, y._1), x._2 + y._2)).filter(x => x._2._2 == expected).map(x => (x._1, x._2._1)))
      var erdd: RDD[Edge[ED]] = firstERDD
      //a special case where we need to filter out edges that are connected to ids for vertices that don't exit
      //TODO: idset might be very large. find a better way to filter
      if (vsem == AggregateSemantics.All && esem == AggregateSemantics.Any) {
        val idset = vrdd.keys.collect.toSet
        erdd = erdd.filter(e => idset.contains(e.srcId) && idset.contains(e.dstId))
      }
      erdd = if (esem == AggregateSemantics.Any) erdd.map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey(eAggFunc).map(x => Edge(x._1._1, x._1._2, x._2)) else erdd.map(e => ((e.srcId, e.dstId), (e.attr, 1))).reduceByKey((x, y) => (eAggFunc(x._1, y._1), x._2 + y._2)).filter(x => x._2._2 == expected).map(x => Edge(x._1._1, x._1._2, x._2._1))

      gps = gps :+ Graph(vrdd, EdgeRDD.fromEdges[ED, VD](erdd))
    }

    new SnapshotGraph(intvs, gps)
  }

  override def transform[ED2: ClassTag, VD2: ClassTag](emap: (Edge[ED], Interval) => ED2, vmap: (VertexId, Interval, VD) => VD2): TemporalGraph[VD2, ED2] = {
    new SnapshotGraph(intervals, graphs.zipWithIndex.map(x => x._1.mapVertices{ (id: VertexId, attr: VD) => vmap(id, intervals(x._2), attr) }.mapEdges{ e: Edge[ED] => emap(e, intervals(x._2))}))
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    new SnapshotGraph(intervals, graphs.zipWithIndex.map(x =>
      x._1.mapVertices{ (id: VertexId, attr: VD) => map(id, intervals(x._2), attr) }))
  }

  override def mapEdges[ED2: ClassTag](map: (Edge[ED], Interval) => ED2): TemporalGraph[VD, ED2] = {
    new SnapshotGraph(intervals, graphs.zipWithIndex.map(x =>
      x._1.mapEdges{ e: Edge[ED] => map(e, intervals(x._2))}))
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, Map[Interval, U])])(mapFunc: (VertexId, Interval, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val newseq: Seq[Graph[VD2, ED]] = graphs.zipWithIndex.map{ x => 
      val intv:Interval = intervals(x._2)
      x._1.outerJoinVertices(other){ (id: VertexId, attr1: VD, attr2: Option[Map[Interval, U]]) =>
        //this function receives a (id, attr, Map)
        //need to instead call mapFunc with (id, index, attr, otherattr)
        if (attr2.isEmpty)
          mapFunc(id, intv, attr1, None)
        else
          mapFunc(id, intv, attr1, attr2.get.get(intv)) }}
    new SnapshotGraph(intervals, newseq)
  }

  @throws(classOf[IllegalArgumentException])
  override def union(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    /** The type checking already validates that the structurally the graphs are union-compatible
      * But we also need to check that they are temporally union-compatible
      * this includes having the same resolution and aligning intervals
      * Two temporal sequences are compatible if their first elements are compatible
      */ 
    var grp2: SnapshotGraph[VD, ED] = other match {
      case grph: SnapshotGraph[VD, ED] => grph
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
      mergedIntervals = mergedIntervals :+ resolution.getInterval(xx)
      xx = mergedIntervals.last.end
    }

    //what we want now is to figure out which indices to place the graphs into
    //in the new sequence
    val graphBuf: Buffer[Graph[VD, ED]] = Buffer[Graph[VD, ED]]()
    var ii:Int = 0
    for (ii <- 0 to mergedIntervals.size) {
      val ind1:Int = intervals.indexOf(mergedIntervals(ii))
      val ind2:Int = grp2.intervals.indexOf(mergedIntervals(ii))
      if (ind1 >= 0 && ind2 >= 0) {
        val gr1 = graphs(ind1)
        val gr2 = grp2.graphs(ind2)
        if (sem == AggregateSemantics.Any) {
          graphBuf(ii) = Graph(VertexRDD(gr1.vertices.union(gr2.vertices).reduceByKey(vFunc)), EdgeRDD.fromEdges[ED, VD](gr1.edges.union(gr2.edges).map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey(eFunc).map { x =>
            val (k, v) = x
            Edge(k._1, k._2, v)
          }))
        } else if (sem == AggregateSemantics.All) {
          graphBuf(ii) = Graph(gr1.vertices.union(gr2.vertices).map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (vFunc(x._1, y._1), x._2 + y._2)).filter(x => x._2._2 == 2).map {x =>
            val (k, v) = x
            (k, v._1)
          }, EdgeRDD.fromEdges[ED, VD](gr1.edges.union(gr2.edges).map(e => ((e.srcId, e.dstId), (e.attr, 1))).reduceByKey((x, y) => (eFunc(x._1, y._1), x._2 + y._2)).filter(x => x._2._2 == 2).map { x =>
            val (k, v) = x
            Edge(k._1, k._2, v._1)
          }))
        }
      } else if (ind1 >= 0 && sem == AggregateSemantics.Any) {
        graphBuf(ii) = graphs(ind1)
      } else if (ind2 >= 0 && sem == AggregateSemantics.Any) {
        graphBuf(ii) = grp2.graphs(ind2)
      } else {
        graphBuf(ii) = Graph[VD, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      }
    }

    new SnapshotGraph(mergedIntervals, graphBuf.toSeq)
  }

  override def intersect(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    /** The type checking already validates that the structurally the graphs are union-compatible
      * But we also need to check that they are temporally union-compatible
      * this includes having the same resolution and aligning intervals
      * Two temporal sequences are compatible if their first elements are compatible
      */ 
    var grp2: SnapshotGraph[VD, ED] = other match {
      case grph: SnapshotGraph[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (!intervals.head.isUnionCompatible(grp2.intervals.head)) {
      throw new IllegalArgumentException("two graphs are not union-compatible in the temporal schema")
    }

    //compute the combined span
     val startBound = if (span.start.isBefore(grp2.span.start)) grp2.span.start else span.start
    val endBound = if (span.end.isAfter(grp2.span.end)) grp2.span.end else span.end
    if (startBound.isAfter(endBound) || startBound.isEqual(endBound)) {
      SnapshotGraph.emptyGraph[VD,ED]()
    } else {
      //compute the new interval
      var mergedIntervals: Seq[Interval] = Seq[Interval]()
      var xx:LocalDate = startBound
      while (xx.isBefore(endBound)) {
        mergedIntervals = mergedIntervals :+ resolution.getInterval(xx)
        xx = mergedIntervals.last.end
      }

      //what we want now is to figure out which indices to place the graphs into
      //in the new sequence
      val graphBuf: Buffer[Graph[VD, ED]] = Buffer[Graph[VD, ED]]()
      var ii:Int = 0
      for (ii <- 0 to mergedIntervals.size) {
        val ind1:Int = intervals.indexOf(mergedIntervals(ii))
        val ind2:Int = grp2.intervals.indexOf(mergedIntervals(ii))
        if (ind1 >= 0 && ind2 >= 0) {
          val gr1 = graphs(ind1)
          val gr2 = grp2.graphs(ind2)
          if (sem == AggregateSemantics.Any) {
            graphBuf(ii) = Graph(VertexRDD(gr1.vertices.union(gr2.vertices).reduceByKey(vFunc)), EdgeRDD.fromEdges[ED, VD](gr1.edges.union(gr2.edges).map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey(eFunc).map { x =>
              val (k, v) = x
              Edge(k._1, k._2, v)
            }))
          } else if (sem == AggregateSemantics.All) {
            graphBuf(ii) = Graph(gr1.vertices.union(gr2.vertices).map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (vFunc(x._1, y._1), x._2 + y._2)).filter(x => x._2._2 == 2).map {x =>
              val (k, v) = x
              (k, v._1)
            }, EdgeRDD.fromEdges[ED, VD](gr1.edges.union(gr2.edges).map(e => ((e.srcId, e.dstId), (e.attr, 1))).reduceByKey((x, y) => (eFunc(x._1, y._1), x._2 + y._2)).filter(x => x._2._2 == 2).map { x =>
              val (k, v) = x
              Edge(k._1, k._2, v._1)
            }))
          }
        } else if (ind1 >= 0 && sem == AggregateSemantics.Any) {
          graphBuf(ii) = graphs(ind1)
        } else if (ind2 >= 0 && sem == AggregateSemantics.Any) {
          graphBuf(ii) = grp2.graphs(ind2)
        } else {
          graphBuf(ii) = Graph[VD, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
        }
      }

      new SnapshotGraph(mergedIntervals, graphBuf.toSeq)
    }
  }

  override def pregel[A: ClassTag]
     (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
       sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): TemporalGraph[VD, ED] = {
    val newseq: Seq[Graph[VD, ED]] = graphs.map(x => Pregel(x, initialMsg,
      maxIterations, activeDirection)(vprog, sendMsg, mergeMsg))
    new SnapshotGraph(intervals, newseq)
  }

  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): TemporalGraph[Double, Double] = {
    var gps: Seq[Graph[Double, Double]] = Seq[Graph[Double, Double]]()

    graphs.foreach { x =>
      if (x.edges.isEmpty) {
        gps = gps :+ Graph[Double, Double](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        if (uni)
          gps = gps :+ UndirectedPageRank.run(Graph(x.vertices, x.edges), tol, resetProb, numIter)
        else if (numIter < Int.MaxValue)
          gps = gps :+ x.staticPageRank(numIter, resetProb)
        else
          gps = gps :+ x.pageRank(tol, resetProb)
      }
    }

    new SnapshotGraph(intervals, gps)
  }

  override def degree(): TemporalGraph[Double,Double] = {
    var gps: Seq[Graph[Double,Double]] = Seq[Graph[Double,Double]]()

    graphs.foreach { x =>
      gps = gps :+ x.outerJoinVertices(x.degrees) { (vid, data, deg) => deg.getOrElse(0).toDouble}.mapEdges(e => 0.0)
    }

    new SnapshotGraph(intervals, gps)
  }
  
  def connectedComponents(): TemporalGraph[VertexId, ED] = {
    var gps: Seq[Graph[Long, ED]] = Seq[Graph[Long, ED]]()

    graphs.foreach { x =>
      if (x.vertices.isEmpty) {
        gps = gps :+ Graph[Long, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        var graph = x.connectedComponents()
        gps = gps :+ graph
      }
    }
    
    new SnapshotGraph(intervals, gps)
  }

  def shortestPaths(landmarks: Seq[VertexId]): TemporalGraph[ShortestPathsXT.SPMap, ED] = {
    var gps: Seq[Graph[ShortestPathsXT.SPMap, ED]] = Seq[Graph[ShortestPathsXT.SPMap, ED]]()

    graphs.foreach { x =>
      if (x.vertices.isEmpty) {
        gps = gps :+ Graph[ShortestPathsXT.SPMap, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        var graph = ShortestPathsXT.run(x, landmarks)
        gps = gps :+ graph
      }
    }
    
    new SnapshotGraph(intervals, gps)
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    val iter: Iterator[Graph[VD, ED]] = graphs.iterator
    var allps: Array[Partition] = Array[Partition]()
    while (iter.hasNext) {
      val g = iter.next
      if (!g.edges.isEmpty) {
        allps = allps union g.edges.partitions
      }
    }
    allps.size
  }

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): TemporalGraph[VD, ED] = {
    //persist each graph
    val iter = graphs.iterator
    while (iter.hasNext)
      iter.next.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): TemporalGraph[VD, ED] = {
    val iter = graphs.iterator
    while (iter.hasNext)
      iter.next.unpersist(blocking)
    this
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int): TemporalGraph[VD, ED] = {
    partitionBy(pst, runs, 0) //0 will make sure that the graph is partitioned with however many partitions it has now
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): TemporalGraph[VD, ED] = {
    if (pst != PartitionStrategyType.None) {
      var gps: Seq[Graph[VD, ED]] = Seq[Graph[VD, ED]]()

      val numParts: Int = if (parts > 0) parts else numPartitions()
      //we partition all snapshots individually here but we should use the total number of partitions as the number
      graphs.zipWithIndex.foreach { case (k,v) =>
        gps = gps :+ k.partitionBy(PartitionStrategies.makeStrategy(pst, v, graphs.size, runs), numParts)
      }

      new SnapshotGraph(intervals, gps)
    } else
      this
  }

}

object SnapshotGraph {
  //this assumes that the data is in the dataPath directory, each time period in its own files
  //end is not inclusive, i.e. [start, end)
  final def loadData(dataPath: String, start:LocalDate , end:LocalDate): SnapshotGraph[String, Int] = {
    var minDate: LocalDate = start
    var maxDate: LocalDate = end

    var source:scala.io.Source = null
    var fs:FileSystem = null

    val pt:Path = new Path(dataPath + "/Span.txt")
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
      minDate =  minin
    if (maxin.isBefore(end)) 
      maxDate = maxin
    source.close()      

    var intvs: Seq[Interval] = Seq[Interval]()
    var gps: Seq[Graph[String, Int]] = Seq[Graph[String, Int]]()
    var xx:LocalDate = minDate

    while (xx.isBefore(maxDate)) {
      val users: RDD[(VertexId, String)] = ProgramContext.sc.textFile(dataPath + "/nodes/nodes" + xx.toString() + ".txt").map(line => line.split(",")).map { parts =>
        if (parts.size > 1 && parts.head != "")
          (parts.head.toLong, parts(1).toString)
        else
          (0L, "Default")
      }
      var edges: RDD[Edge[Int]] = ProgramContext.sc.emptyRDD
    
      val ept:Path = new Path(dataPath + "/edges/edges" + xx.toString() + ".txt")
      if (fs.exists(ept) && fs.getFileStatus(ept).getLen > 0) {
        //uses extended version of Graph Loader to load edges with attributes
        val tmp = xx.toString()
        edges = GraphLoaderAddon.edgeListFile(ProgramContext.sc, dataPath + "/edges/edges" + tmp + ".txt", true).edges
      } else {
        edges = ProgramContext.sc.emptyRDD
      }
      
      intvs = intvs :+ res.getInterval(xx)
      gps = gps :+ Graph(users, EdgeRDD.fromEdges(edges))
      xx = intvs.last.end
    }

    new SnapshotGraph(intvs, gps)
  }

  def emptyGraph[VD: ClassTag, ED: ClassTag]():SnapshotGraph[VD, ED] = new SnapshotGraph(Seq[Interval](), Seq[Graph[VD, ED]]())
}
