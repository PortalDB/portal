package edu.drexel.cs.dbgroup.graphxt

import scala.collection.parallel.ParSeq
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

import edu.drexel.cs.dbgroup.graphxt.util.MultifileLoad
import edu.drexel.cs.dbgroup.graphxt.util.NumberRangeRegex

import java.time.LocalDate

class SnapshotGraphParallel[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], gps: ParSeq[Graph[VD, ED]]) extends TemporalGraph[VD, ED] with Serializable {
  val graphs: ParSeq[Graph[VD, ED]] = gps
  //because intervals are consecutive and equally sized,
  //we could store just the start of each one
  val resolution:Resolution = if (intvs.size > 0) intvs.head.resolution else Resolution.zero

  intvs.foreach { ii =>
    if (!ii.resolution.isEqual(resolution))
      throw new IllegalArgumentException("temporal sequence is not valid, intervals are not all of equal resolution")
  }

  val intervals: Seq[Interval] = intvs
  lazy val span: Interval = if (intervals.size > 0) Interval(intvs.head.start, intvs.last.end) else Interval(LocalDate.now, LocalDate.now)

  /** Default constructor is provided to support serialization */
  protected def this() = this(Seq[Interval](), ParSeq[Graph[VD, ED]]())

  override def size(): Int = graphs.size

  override def materialize() = {
    graphs.foreach { x =>
      if (!x.edges.isEmpty)
        x.numEdges
      if (!x.vertices.isEmpty)
        x.numVertices
    }
  }

  override def vertices: VertexRDD[Map[Interval, VD]] = {
    if (size > 0) {
      val total = graphs.zipWithIndex.map(x => (x._1, intervals(x._2))).filterNot(x => x._1.vertices.isEmpty)
      if (total.size > 0)
        VertexRDD(total.map(x => x._1.vertices.mapValues(y => Map[Interval, VD](x._2 -> y)))
          .reduce((a: RDD[(VertexId,Map[Interval,VD])], b: RDD[(VertexId,Map[Interval,VD])]) => a union b)
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
      val total = graphs.zipWithIndex
        .map(x => (x._1, intervals(x._2)))
        .filterNot(x => x._1.vertices.isEmpty)
      if (total.size > 0)
        VertexRDD(total.map(x => x._1.vertices.mapValues(y => (x._2, y)))
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
      val total = graphs.zipWithIndex
        .map(x => (x._1, intervals(x._2)))
        .filterNot(x => x._1.edges.isEmpty)
      if (total.size > 0)
        EdgeRDD.fromEdges[Map[Interval, ED], VD](total
          .map(x => x._1.edges.mapValues(y => Map[Interval, ED](x._2 -> y.attr)))
          .reduce((a: RDD[Edge[Map[Interval, ED]]], b: RDD[Edge[Map[Interval, ED]]]) => a union b)
          .map(x => ((x.srcId, x.dstId), x.attr))
          .reduceByKey((a: Map[Interval, ED], b: Map[Interval, ED]) => a ++ b)
          .map(x => Edge(x._1._1, x._1._2, x._2))
        )
      else
        EdgeRDD.fromEdges[Map[Interval, ED], VD](ProgramContext.sc.emptyRDD)
    } else
      EdgeRDD.fromEdges[Map[Interval, ED], VD](ProgramContext.sc.emptyRDD)
  }

  override def edgesFlat: EdgeRDD[(Interval, ED)] = {
    if (size > 0) {
      val total = graphs.zipWithIndex
        .map(x => (x._1, intervals(x._2)))
        .filterNot(x => x._1.edges.isEmpty)
      if (total.size > 0)
        EdgeRDD.fromEdges[(Interval, ED), VD](total
          .map(x => x._1.edges.mapValues(y => (x._2,y.attr)))
          .reduce((a: RDD[Edge[(Interval, ED)]], b: RDD[Edge[(Interval, ED)]]) => a union b))
      else
        EdgeRDD.fromEdges[(Interval, ED), VD](ProgramContext.sc.emptyRDD)
    } else
        EdgeRDD.fromEdges[(Interval, ED), VD](ProgramContext.sc.emptyRDD)
  }

  override def degrees: VertexRDD[Map[Interval, Int]] = {
    if (size > 0) {
      val total = graphs.zipWithIndex
        .map(x => (x._1, intervals(x._2)))
        .filterNot(x => x._1.edges.isEmpty)
        .map(x => x._1.degrees.mapValues(deg => Map[Interval, Int](x._2 -> deg)))
      if (total.size > 0)
        VertexRDD(total
          .reduce((x,y) => VertexRDD(x union y))
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
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this

    if (!span.intersects(bound)) {
      return SnapshotGraphParallel.emptyGraph[VD,ED]()
    }

    val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
    val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end

    //compute indices of start and stop
    val selectStart:Int = intervals.indexOf(resolution.getInterval(startBound))
    var selectStop:Int = intervals.indexOf(resolution.getInterval(endBound))
    if (selectStop < 0) selectStop = intervals.size

    val newIntvs: Seq[Interval] = intervals.slice(selectStart, selectStop)
    val newGraphs: ParSeq[Graph[VD, ED]] = graphs.slice(selectStart, selectStop)

    new SnapshotGraphParallel(newIntvs, newGraphs)
  }

  override def select(tpred: Interval => Boolean): TemporalGraph[VD, ED] = {
    new SnapshotGraphParallel(intervals, graphs.zipWithIndex.map{case (g,i) =>
      if (tpred(intervals(i)))
        g
      else
        Graph[VD, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
    })
  }

  override def select(epred: EdgeTriplet[VD,ED] => Boolean, vpred: (VertexId, VD) => Boolean): TemporalGraph[VD, ED] = {
    new SnapshotGraphParallel(intervals, graphs.map(x => x.subgraph(epred, vpred)))
  }

  override def aggregate(res: Resolution, sem: AggregateSemantics.Value, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    if (size == 0)
      return SnapshotGraphParallel.emptyGraph[VD,ED]()

    var intvs: Seq[Interval] = Seq[Interval]()
    var gps: ParSeq[Graph[VD, ED]] = ParSeq[Graph[VD, ED]]()

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
      if (sem == AggregateSemantics.Existential) {
        gps = gps :+ Graph(VertexRDD(firstVRDD.reduceByKey(vAggFunc)), EdgeRDD.fromEdges[ED, VD](firstERDD.map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey(eAggFunc).map { x =>
          val (k, v) = x
          Edge(k._1, k._2, v)
        }))
      } else if (sem == AggregateSemantics.Universal && num == expected) {
        //we only have a valid aggregated snapshot if we have all the datapoints from this interval
        gps = gps :+ Graph(firstVRDD.map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (vAggFunc(x._1, y._1), x._2 + y._2)).filter(x => x._2._2 == expected).map { x =>
          val (k, v) = x
          (k, v._1)
        },
          EdgeRDD.fromEdges[ED, VD](firstERDD.map(e => ((e.srcId, e.dstId), (e.attr, 1))).reduceByKey((x, y) => (eAggFunc(x._1, y._1), x._2 + y._2)).filter(x => x._2._2 == expected).map { x =>
            val (k, v) = x
            Edge(k._1, k._2, v._1)
          }))
      } else { //empty graph
        gps = gps :+ Graph[VD, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      }
    }

    new SnapshotGraphParallel(intvs, gps)
  }

  override def transform[ED2: ClassTag, VD2: ClassTag](emap: (Edge[ED], Interval) => ED2, vmap: (VertexId, Interval, VD) => VD2): TemporalGraph[VD2, ED2] = {
    new SnapshotGraphParallel(intervals, graphs.zipWithIndex
      .map(x => (x._1, intervals(x._2)))
      .map(x => x._1.mapVertices{ (id: VertexId, attr: VD) => vmap(id, x._2, attr) }.mapEdges{ e: Edge[ED] => emap(e, x._2)}))
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    new SnapshotGraphParallel(intervals, graphs.zipWithIndex
      .map(x => (x._1, intervals(x._2)))
      .map(x =>
      x._1.mapVertices{ (id: VertexId, attr: VD) => map(id, x._2, attr) }))
  }

  override def mapEdges[ED2: ClassTag](map: (Edge[ED], Interval) => ED2): TemporalGraph[VD, ED2] = {
    new SnapshotGraphParallel(intervals, graphs.zipWithIndex
      .map(x => (x._1, intervals(x._2)))
      .map(x =>
      x._1.mapEdges{ e: Edge[ED] => map(e, x._2)}))
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, Map[Interval, U])])(mapFunc: (VertexId, Interval, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    new SnapshotGraphParallel(intervals, graphs.zipWithIndex.map {x => 
      val in:Interval = intervals(x._2)
      x._1.outerJoinVertices(other){ (id: VertexId, attr1: VD, attr2: Option[Map[Interval, U]]) =>
        //this function receives a (id, attr, Map)
        //need to instead call mapFunc with (id, index, attr, otherattr)
        if (attr2.isEmpty)
          mapFunc(id, in, attr1, None)
        else
          mapFunc(id, in, attr1, attr2.get.get(in)) }})
  }

  override def union(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    /** The type checking already validates that the structurally the graphs are union-compatible
      * But we also need to check that they are temporally union-compatible
      * this includes having the same resolution and aligning intervals
      * Two temporal sequences are compatible if their first elements are compatible
      */ 
    var grp2: SnapshotGraphParallel[VD, ED] = other match {
      case grph: SnapshotGraphParallel[VD, ED] => grph
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

    //make graph sequences of the appropriate length
    val gr1IndexStart:Long = resolution.numBetween(startBound, span.start)
    val gr2IndexStart:Long = resolution.numBetween(startBound, grp2.span.start)
    val gr1Pad:Long = resolution.numBetween(span.end, endBound)
    val gr2Pad:Long = resolution.numBetween(grp2.span.end, endBound)
    val grseq1start:ParSeq[Graph[VD, ED]] = ParSeq.fill(gr1IndexStart.toInt){Graph[VD, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)}
    val grseq1pad:ParSeq[Graph[VD, ED]] = ParSeq.fill(gr1Pad.toInt){Graph[VD, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)}
    val grseq1:ParSeq[Graph[VD, ED]] =  grseq1start ++ graphs ++ grseq1pad

    val grseq2start:ParSeq[Graph[VD, ED]] = ParSeq.fill(gr2IndexStart.toInt){Graph[VD, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)}
    val grseq2pad:ParSeq[Graph[VD, ED]] = ParSeq.fill(gr2Pad.toInt){Graph[VD, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)}
    val grseq2:ParSeq[Graph[VD, ED]] = grseq2start ++ grp2.graphs ++ grseq2pad

    //then zip them
    val mergedGraphs:ParSeq[Graph[VD, ED]] = grseq1.zip(grseq2).map { twogrs =>
      val (gr1:Graph[VD,ED],gr2:Graph[VD,ED]) = twogrs
      if (gr1.vertices.isEmpty) {
        if (sem == AggregateSemantics.Existential)
          gr2
        else
          Graph[VD, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else if (gr2.vertices.isEmpty) {
        if (sem == AggregateSemantics.Existential)
          gr1
        else
          Graph[VD, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        if (sem == AggregateSemantics.Existential) {
          Graph(VertexRDD(gr1.vertices.union(gr2.vertices).reduceByKey(vFunc)), EdgeRDD.fromEdges[ED, VD](gr1.edges.union(gr2.edges).map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey(eFunc).map { x =>
            val (k, v) = x
            Edge(k._1, k._2, v)
          }))
        } else { //universal
          Graph(gr1.vertices.union(gr2.vertices).map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (vFunc(x._1, y._1), x._2 + y._2)).filter(x => x._2._2 == 2).map {x =>
            val (k, v) = x
            (k, v._1)
          }, EdgeRDD.fromEdges[ED, VD](gr1.edges.union(gr2.edges).map(e => ((e.srcId, e.dstId), (e.attr, 1))).reduceByKey((x, y) => (eFunc(x._1, y._1), x._2 + y._2)).filter(x => x._2._2 == 2).map { x =>
            val (k, v) = x
            Edge(k._1, k._2, v._1)
          }))
        }
      }
    }

    new SnapshotGraphParallel(mergedIntervals, mergedGraphs)
  }

  override def intersection(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    /** The type checking already validates that the structurally the graphs are union-compatible
      * But we also need to check that they are temporally union-compatible
      * this includes having the same resolution and aligning intervals
      * Two temporal sequences are compatible if their first elements are compatible
      */ 
    var grp2: SnapshotGraphParallel[VD, ED] = other match {
      case grph: SnapshotGraphParallel[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (!intervals.head.isUnionCompatible(grp2.intervals.head)) {
      throw new IllegalArgumentException("two graphs are not union-compatible in the temporal schema")
    }

    //compute the combined span
     val startBound = if (span.start.isBefore(grp2.span.start)) grp2.span.start else span.start
    val endBound = if (span.end.isAfter(grp2.span.end)) grp2.span.end else span.end
    if (startBound.isAfter(endBound) || startBound.isEqual(endBound)) {
      SnapshotGraphParallel.emptyGraph[VD,ED]()
    } else {
      //compute the new interval
      var mergedIntervals: Seq[Interval] = Seq[Interval]()
      var xx:LocalDate = startBound
      while (xx.isBefore(endBound)) {
        mergedIntervals = mergedIntervals :+ resolution.getInterval(xx)
        xx = mergedIntervals.last.end
      }

      //make graph sequences of the appropriate length
      val gr1IndexStart:Int = resolution.numBetween(span.start, startBound).toInt
      val gr2IndexStart:Int = resolution.numBetween(grp2.span.start, startBound).toInt
      val gr1IndexEnd:Int = gr1IndexStart + mergedIntervals.size
      val gr2IndexEnd:Int = gr2IndexStart + mergedIntervals.size
      val grseq1:ParSeq[Graph[VD, ED]] = graphs.slice(gr1IndexStart, gr1IndexEnd)
      val grseq2:ParSeq[Graph[VD, ED]] = grp2.graphs.slice(gr2IndexStart, gr2IndexEnd)

      //then zip them
      val mergedGraphs:ParSeq[Graph[VD, ED]] = grseq1.zip(grseq2).map { twogrs =>
        val (gr1:Graph[VD,ED],gr2:Graph[VD,ED]) = twogrs
        if (gr1.vertices.isEmpty) {
          if (sem == AggregateSemantics.Existential)
            gr2
          else
            Graph[VD, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
        } else if (gr2.vertices.isEmpty) {
          if (sem == AggregateSemantics.Existential)
            gr1
          else
            Graph[VD, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
        } else {
          if (sem == AggregateSemantics.Existential) {
            Graph(VertexRDD(gr1.vertices.union(gr2.vertices).reduceByKey(vFunc)), EdgeRDD.fromEdges[ED, VD](gr1.edges.union(gr2.edges).map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey(eFunc).map { x =>
              val (k, v) = x
              Edge(k._1, k._2, v)
            }))
          } else { //universal
            Graph(gr1.vertices.union(gr2.vertices).map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (vFunc(x._1, y._1), x._2 + y._2)).filter(x => x._2._2 == 2).map {x =>
              val (k, v) = x
              (k, v._1)
            }, EdgeRDD.fromEdges[ED, VD](gr1.edges.union(gr2.edges).map(e => ((e.srcId, e.dstId), (e.attr, 1))).reduceByKey((x, y) => (eFunc(x._1, y._1), x._2 + y._2)).filter(x => x._2._2 == 2).map { x =>
              val (k, v) = x
              Edge(k._1, k._2, v._1)
            }))
          }
        }
      }

      new SnapshotGraphParallel(mergedIntervals, mergedGraphs)
    }
  }

  override def pregel[A: ClassTag]
     (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
       sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): TemporalGraph[VD, ED] = {
    new SnapshotGraphParallel(intervals, graphs.map(x => Pregel(x, initialMsg,
      maxIterations, activeDirection)(vprog, sendMsg, mergeMsg)))
  }

  //run PageRank on each contained snapshot
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): TemporalGraph[Double, Double] = {
      def safePagerank(grp: Graph[VD, ED]): Graph[Double, Double] = {
        if (grp.edges.isEmpty) {
          Graph[Double, Double](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
        } else {
          if (uni) {
            UndirectedPageRank.run(grp, tol, resetProb, numIter)
          } else if (numIter < Int.MaxValue)
            grp.staticPageRank(numIter, resetProb)
          else
            grp.pageRank(tol, resetProb)
        }
      }

    new SnapshotGraphParallel(intervals, graphs.map(safePagerank))

  }

  override def connectedComponents(): TemporalGraph[VertexId, ED] = {
    def safeConnectedComponents(grp: Graph[VD, ED]): Graph[VertexId, ED] = {
      if (grp.vertices.isEmpty) {
        Graph[VertexId, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        grp.connectedComponents()
      }
    }

    new SnapshotGraphParallel(intervals, graphs.map(safeConnectedComponents))
  }

  override def shortestPaths(landmarks: Seq[VertexId]): TemporalGraph[ShortestPathsXT.SPMap, ED] = {
    def safeShortestPaths(grp: Graph[VD, ED]): Graph[ShortestPathsXT.SPMap, ED] = {
      if (grp.vertices.isEmpty) {
        Graph[ShortestPathsXT.SPMap, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        ShortestPathsXT.run(grp, landmarks)
      }
    }

    new SnapshotGraphParallel(intervals, graphs.map(safeShortestPaths))
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    graphs.filterNot(_.edges.isEmpty).map(_.edges.partitions.size).reduce(_ + _)
  }

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): TemporalGraph[VD, ED] = {
    //persist each graph
    graphs.map(_.persist(newLevel))
    this
  }

  override def unpersist(blocking: Boolean = true): TemporalGraph[VD, ED] = {
    graphs.map(_.unpersist(blocking))
    this
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int): TemporalGraph[VD, ED] = {
    partitionBy(pst, runs, 0)
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): TemporalGraph[VD, ED] = {
    if (pst != PartitionStrategyType.None) {
      //not changing the intervals, only the graphs at their indices
      //each partition strategy for SG needs information about the graph

      //use that strategy to partition each of the snapshots
      new SnapshotGraphParallel(intervals, graphs.zipWithIndex.map { case (g,i) =>
        val numParts: Int = if (parts > 0) parts else g.edges.partitions.size
        g.partitionBy(PartitionStrategies.makeStrategy(pst, i, graphs.size, runs), numParts)
      })
    } else
      this
  }

}

object SnapshotGraphParallel extends Serializable {
  //this assumes that the data is in the dataPath directory, each time period in its own files
  //end is not inclusive, i.e. [start, end)
  final def loadData(dataPath: String, start:LocalDate, end:LocalDate): SnapshotGraphParallel[String, Int] = {
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

    var intvs: Seq[Interval] = Seq[Interval]()
    var gps: ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]]()
    var xx:LocalDate = minDate

    while (xx.isBefore(maxDate)) {
      var nodesPath = dataPath + "/nodes/nodes" + xx.toString() + ".txt"
      var edgesPath = dataPath + "/edges/edges" + xx.toString() + ".txt"
      var numNodeParts = MultifileLoad.estimateParts(nodesPath) 
      var numEdgeParts = MultifileLoad.estimateParts(edgesPath) 
      
      val users: RDD[(VertexId, String)] = ProgramContext.sc.textFile(dataPath + "/nodes/nodes" + xx.toString() + ".txt", numNodeParts).map(line => line.split(",")).map { parts =>
        if (parts.size > 1 && parts.head != "")
          (parts.head.toLong, parts(1).toString)
        else
          (0L, "Default")
      }
      
      var edges: EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.emptyRDD)

      val ept: Path = new Path(dataPath + "/edges/edges" + xx.toString() + ".txt")
      if (fs.exists(ept) && fs.getFileStatus(ept).getLen > 0) {
        //uses extended version of Graph Loader to load edges with attributes
        edges = GraphLoaderAddon.edgeListFile(ProgramContext.sc, dataPath + "/edges/edges" + xx.toString() + ".txt", true, numEdgeParts).edges
      }
      
      intvs = intvs :+ res.getInterval(xx)
      gps = gps :+ Graph(users, edges)
      xx = intvs.last.end
    }
    
    new SnapshotGraphParallel(intvs, gps)
  }

  final def loadWithPartition(dataPath: String, start: LocalDate, end: LocalDate, strategy: PartitionStrategyType.Value, runWidth: Int): SnapshotGraphParallel[String, Int] = {
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

    var intvs: Seq[Interval] = Seq[Interval]()
    var gps: ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]]()
    var xx:LocalDate = minDate

    val total = res.numBetween(minDate, maxDate)
    //val numParts = MultifileLoad.estimateParts(dataPath + "/edges/edges{" + NumberRangeRegex.generateRegex(minDate.getYear(), maxDate.getYear()) + "}-{*}.txt")

    while (xx.isBefore(maxDate)) {
      var nodesPath = dataPath + "/nodes/nodes" + xx.toString() + ".txt"
      var edgesPath = dataPath + "/edges/edges" + xx.toString() + ".txt"
      var numNodeParts = MultifileLoad.estimateParts(nodesPath) 
      var numEdgeParts = MultifileLoad.estimateParts(edgesPath) 
      
      val users: RDD[(VertexId, String)] = ProgramContext.sc.textFile(dataPath + "/nodes/nodes" + xx.toString() + ".txt", numNodeParts).map(line => line.split(",")).map { parts =>
        if (parts.size > 1 && parts.head != "")
          (parts.head.toLong, parts(1).toString)
        else
          (0L, "Default")
      }
      
      var edges: EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.emptyRDD)

      val ept: Path = new Path(dataPath + "/edges/edges" + xx.toString() + ".txt")
      if (fs.exists(ept) && fs.getFileStatus(ept).getLen > 0) {
        //uses extended version of Graph Loader to load edges with attributes
        var g = GraphLoaderAddon.edgeListFile(ProgramContext.sc, dataPath + "/edges/edges" + xx.toString() + ".txt", true, numEdgeParts)
        if (strategy != PartitionStrategyType.None) {
          g = g.partitionBy(PartitionStrategies.makeStrategy(strategy, intvs.size + 1, total, runWidth), g.edges.partitions.size)
        }
        edges = g.edges
      }
      
      intvs = intvs :+ res.getInterval(xx)
      gps = gps :+ Graph(users, edges)
      xx = intvs.last.end
    }

    new SnapshotGraphParallel(intvs, gps)
  }

  def emptyGraph[VD: ClassTag, ED: ClassTag]():SnapshotGraphParallel[VD, ED] = new SnapshotGraphParallel(Seq[Interval](), ParSeq[Graph[VD, ED]]())
}
