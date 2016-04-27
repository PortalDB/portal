package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.parallel.ParSeq
import scala.collection.immutable.BitSet
import scala.collection.mutable.LinkedHashMap
import scala.collection.breakOut

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

class HybridGraph[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[((VertexId, VertexId), (Interval, ED))], runs: Seq[Int], gps: ParSeq[Graph[BitSet, BitSet]], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY) extends TGraphNoSchema[VD, ED](intvs, verts, edgs, defValue, storLevel) with Serializable {

  val graphs: ParSeq[Graph[BitSet, BitSet]] = gps
  //this is how many consecutive intervals are in each aggregated graph
  val widths: Seq[Int] = runs
  
  if (widths.reduce(_ + _) != intvs.size)
    throw new IllegalArgumentException("temporal sequence and runs do not match")

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
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this

    if (span.intersects(bound)) {
      val startBound = maxDate(span.start, bound.start)
      val endBound = minDate(span.end, bound.end)
      val selectBound:Interval = Interval(startBound, endBound)
      val start = span.start

      //compute indices of start and stop
      //start is inclusive, stop exclusive
      val selectStart:Int = intervals.indexWhere(intv => intv.intersects(selectBound))
      var selectStop:Int = intervals.lastIndexWhere(intv => intv.intersects(selectBound))
      if (selectStop < 0) selectStop = intervals.size
      val newIntvs: Seq[Interval] = intervals.slice(selectStart, selectStop)

      //compute indices of the aggregates that should be included
      //both inclusive
      val partialSum: Seq[Int] = widths.scanLeft(0)(_ + _).tail
      val indexStart: Int = partialSum.indexWhere(_ >= (selectStart+1))
      val indexStop: Int = partialSum.indexWhere(_ >= selectStop)

      //TODO: rewrite simpler
      val tail: Seq[Int] = if (indexStop == indexStart) Seq() else Seq(widths(indexStop) - partialSum(indexStop) + selectStop)
      val runs: Seq[Int] = if (indexStop == indexStart) Seq(selectStop - selectStart) else Seq(partialSum(indexStart) - selectStart) ++ widths.slice(indexStart, indexStop).drop(1) ++ tail
      //indices to keep in partial graphs
      val stop1: Int = partialSum(indexStart) - 1
      val stop2: Int = partialSum.lift(indexStop-1).getOrElse(0)

      //filter out aggregates where we need only partials
      //drop those we don't need
      //keep the rest
      val subg:ParSeq[Graph[BitSet,BitSet]] = graphs.zipWithIndex.flatMap{ case (g,index) =>
        if (index == indexStart || index == indexStop) {
          val mask: BitSet = if (index == indexStart) BitSet((selectStart to stop1): _*) else BitSet((stop2 to (selectStop-1)): _*)
          Some(g.subgraph(
            vpred = (vid, attr) => !(attr & mask).isEmpty,
            epred = et => !(et.attr & mask).isEmpty)
            .mapVertices((vid, vattr) => vattr.filter(x => x >= selectStart && x < selectStop).map(_ - selectStart)).mapEdges(e => e.attr.filter(x => x > selectStart && x < selectStop).map(_ - selectStart)))
        } else if (index > indexStart && index < indexStop) {
          Some(g.mapVertices((vid, vattr) => vattr.map(_ - selectStart))
            .mapEdges(e => e.attr.map(_ - selectStart)))
        } else
          None
      }

      //now need to update the vertex attribute rdd and edge attr rdd
      val vattrs = allVertices.filter{ case (k,v) => v._1.intersects(selectBound)}.mapValues( v => (Interval(maxDate(v._1.start, startBound), minDate(v._1.end, endBound)), v._2))
      val eattrs = allEdges.filter{ case (k,v) => v._1.intersects(selectBound)}.mapValues( v => (Interval(maxDate(v._1.start, startBound), minDate(v._1.end, endBound)), v._2))

      new HybridGraph[VD, ED](newIntvs, vattrs, eattrs, runs, subg, defaultValue, storageLevel)

    } else
      HybridGraph.emptyGraph[VD,ED](defaultValue)
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
    def mergeFunc(a:LinkedHashMap[TimeIndex,Int], b:LinkedHashMap[TimeIndex,Int]): LinkedHashMap[TimeIndex,Int] = {
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    val degRDDs = graphs.map(g => g.aggregateMessages[LinkedHashMap[TimeIndex, Int]](
      ctx => {
        ctx.sendToSrc(LinkedHashMap[TimeIndex,Int]() ++ ctx.attr.seq.map(x => (x,1)))
        ctx.sendToDst(LinkedHashMap[TimeIndex,Int]() ++ ctx.attr.seq.map(x => (x,1)))
      },
      mergeFunc, TripletFields.None)
    )

    val intvs = intervals
    TGraphNoSchema.coalesce(degRDDs.reduce((x: RDD[(VertexId, LinkedHashMap[TimeIndex, Int])], y: RDD[(VertexId, LinkedHashMap[TimeIndex, Int])]) => x union y).flatMap{ case (vid, map) => map.map{ case (k,v) => (vid, (intvs(k), v))}})
  }


  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): HybridGraph[Double, Double] = {
    val runSums = widths.scanLeft(0)(_ + _).tail

    if (uni) {
      def prank(grp: Graph[BitSet,BitSet], minIndex: Int, maxIndex: Int): Graph[LinkedHashMap[TimeIndex,(Double,Double)], LinkedHashMap[TimeIndex,(Double,Double)]] = {
        if (grp.edges.isEmpty)
          Graph[LinkedHashMap[TimeIndex,(Double,Double)],LinkedHashMap[TimeIndex,(Double,Double)]](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
        else {
          UndirectedPageRank.runHybrid(grp, minIndex, maxIndex-1, tol, resetProb, numIter)
        }
      }
    
      val allgs:ParSeq[Graph[LinkedHashMap[TimeIndex,(Double,Double)], LinkedHashMap[TimeIndex,(Double,Double)]]] = graphs.zipWithIndex.map{ case (g,i) => prank(g, runSums.lift(i-1).getOrElse(0), runSums(i))}

      //now extract values
      val intvs = intervals
      val vattrs= allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.toSeq.map{ case (k,v) => (vid, (intvs(k), v._1))}}}.reduce(_ union _)
      val eattrs = allgs.map{ g => g.edges.flatMap{ e => e.attr.toSeq.map{ case (k,v) => ((e.srcId, e.dstId), (intvs(k), v._1))}}}.reduce(_ union _)

      new HybridGraph(intervals, vattrs, eattrs, widths, graphs, 0.0, storageLevel)

    } else
      throw new UnsupportedOperationException("directed version of pagerank not yet implemented")
  }

  override def connectedComponents(): HybridGraph[VertexId, ED] = {
    val runSums = widths.scanLeft(0)(_ + _).tail

    def conc(grp: Graph[BitSet,BitSet], minIndex: Int, maxIndex: Int): Graph[LinkedHashMap[TimeIndex,VertexId],BitSet] = {
    if (grp.vertices.isEmpty)
        Graph[LinkedHashMap[TimeIndex,VertexId],BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      else {
        ConnectedComponentsXT.runHybrid(grp, minIndex, maxIndex-1)
      }
    }

    val allgs = graphs.zipWithIndex.map{ case (g,i) => conc(g, runSums.lift(i-1).getOrElse(0), runSums(i))}

    //now extract values
    val intvs = intervals
    val vattrs = allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.toSeq.map{ case (k,v) => (vid, (intvs(k), v))}}}.reduce(_ union _)

    new HybridGraph(intervals, vattrs, allEdges, widths, graphs, -1L, storageLevel)
  }

  override def shortestPaths(landmarks: Seq[VertexId]): HybridGraph[Map[VertexId, Int], ED] = {
    throw new UnsupportedOperationException("shortest paths not yet implemented")
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    graphs.filterNot(_.edges.isEmpty).map(_.edges.partitions.size).reduce(_ + _)
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
  
  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int): HybridGraph[VD, ED] = {
    partitionBy(pst, runs, 0)
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): HybridGraph[VD, ED] = {
    if (pst != PartitionStrategyType.None) {
      new HybridGraph(intervals, allVertices, allEdges, widths, graphs.zipWithIndex.map { case (g,index) =>
        val numParts: Int = if (parts > 0) parts else g.edges.partitions.size
        g.partitionBy(PartitionStrategies.makeStrategy(pst, index, intervals.size, runs), numParts)}, defaultValue, storageLevel)
    } else
      this
  }

  override protected def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY): HybridGraph[V, E] = {
    HybridGraph.fromRDDs(verts, edgs, defVal, storLevel)
  }

  override protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): HybridGraph[V, E] = HybridGraph.emptyGraph(defVal)

}

object HybridGraph extends Serializable {
  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): HybridGraph[V, E] = new HybridGraph(Seq[Interval](), ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, Seq[Int](0), ParSeq[Graph[BitSet,BitSet]](), defVal)

  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, runWidth: Int = 8): HybridGraph[V, E] = {
    val intervals = TGraphNoSchema.computeIntervals(verts, edgs)
    val broadcastIntervals = ProgramContext.sc.broadcast(intervals)

    val combined = intervals.zipWithIndex.grouped(runWidth).map{ intvs =>
      (intvs.size, 
        Graph(verts.filter(v => v._2._1.intersects(Interval(intvs.head._1.start, intvs.last._1.end))).mapValues{v =>
          BitSet() ++ intvs.filter{ case (intv, index) => intv.intersects(v._1)}.map(ii => ii._2)
        }.reduceByKey((a,b) => a union b),
          edgs.filter(e => e._2._1.intersects(Interval(intvs.head._1.start, intvs.last._1.end))).mapValues{e =>
            BitSet() ++ intvs.filter{ case (intv, index) => intv.intersects(e._1)}.map(ii => ii._2)}.reduceByKey((a,b) => a union b).map(e => Edge(e._1._1, e._1._2, e._2)), BitSet(), storLevel)
      )}

    new HybridGraph(intervals, verts, edgs, combined.map(x => x._1).toSeq, combined.map(x => x._2).toSeq.par, defVal, storLevel)

  }
}
