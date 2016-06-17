package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.parallel.ParSeq
import scala.reflect.ClassTag
import scala.util.control._
import scala.language.implicitConversions
import java.time.LocalDate
import java.sql.Date

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import org.apache.spark.SparkContext
import org.apache.spark.Partition
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.storage.{RDDBlockId,StorageLevel}
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame,Row,Column}
import org.apache.spark.sql.types.{StructType,StructField}

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps._

class SnapshotGraphWithSchema(intvs: Seq[Interval], verts: DataFrame, edgs: DataFrame, gps: ParSeq[Graph[Row,Row]], storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraphWithSchema(intvs, verts, edgs, storLevel, coal) {
  import sqlContext.implicits._

  protected val graphs: ParSeq[Graph[Row, Row]] = gps

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

  override def getSnapshot(time: LocalDate): Graph[Row,Row] = {
    val index = intervals.indexWhere(a => a.contains(time))
    if (index >= 0) {
      graphs(index)
    } else
      Graph[Row,Row](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
  }

  /** Query operations */

  override def slice(bound: Interval): SnapshotGraphWithSchema = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this

    if (!span.intersects(bound)) {
      return SnapshotGraphWithSchema.emptyGraph(schema)
    }

    val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
    val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end
    val selectBound:Interval = Interval(startBound, endBound)
    //compute indices of start and stop
    val selectStart:Int = intervals.indexWhere(intv => intv.intersects(selectBound))
    var selectStop:Int = intervals.lastIndexWhere(intv => intv.intersects(selectBound))
    if (selectStop < 0) selectStop = intervals.size
    val newIntvs: Seq[Interval] = intervals.slice(selectStart, selectStop)

    val sb = Date.valueOf(startBound)
    val eb = Date.valueOf(endBound)
    val mnf: (Date => Date) = (in: Date) => if (in.before(eb)) in else eb
    val mxf: (Date => Date) = (in: Date) => if (sb.before(in)) in else sb
    val maxFunc = udf(mxf)
    val minFunc = udf(mnf)
    val clsv = Array(allVertices("vid"), maxFunc($"estart").as("estart"), minFunc($"eend").as("eend")) ++ allVertices.columns.drop(3).map(c => allVertices(c))
    val clse = Array(allEdges("vid1"), allEdges("vid2"), maxFunc($"estart").as("estart"), minFunc($"eend").as("eend")) ++ allEdges.columns.drop(4).map(c => allEdges(c))

    new SnapshotGraphWithSchema(newIntvs, allVertices.where(!($"estart" >= eb || $"eend" <= sb)).select(clsv: _*), 
      allEdges.where(!($"estart" >= eb || $"eend" <= sb)).select(clse: _*), 
      graphs.slice(selectStart, selectStop), storageLevel)

  }

  override def union(other: TGraph[Row, Row], vFunc: (Row, Row) => Row, eFunc: (Row, Row) => Row): SnapshotGraphWithSchema = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def intersection(other: TGraph[Row, Row], vFunc: (Row, Row) => Row, eFunc: (Row, Row) => Row): SnapshotGraphWithSchema = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def pregel[A: ClassTag]
     (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, Row, A) => Row,
       sendMsg: EdgeTriplet[Row, Row] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): SnapshotGraphWithSchema = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def degree: RDD[(VertexId, (Interval, Int))] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    graphs.filterNot(_.edges.isEmpty).map(_.edges.partitions.size).reduce(_ + _)
  }

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): SnapshotGraphWithSchema = {
    super.persist(newLevel)
    //persist each graph
    //this will throw an exception if the graphs are already persisted
    //with a different storage level
    graphs.map(g => g.persist(newLevel))
    this
  }

  override def unpersist(blocking: Boolean = true): SnapshotGraphWithSchema = {
    super.unpersist(blocking)
    graphs.map(_.unpersist(blocking))
    this
  }

  override def partitionBy(tgp: TGraphPartitioning): SnapshotGraphWithSchema = {
    if (tgp.pst != PartitionStrategyType.None) {
      //not changing the intervals, only the graphs at their indices
      //each partition strategy for SG needs information about the graph

      //use that strategy to partition each of the snapshots
      new SnapshotGraphWithSchema(intervals, allVertices, allEdges, graphs.zipWithIndex.map { case (g,i) =>
        val numParts: Int = if (tgp.parts > 0) tgp.parts else g.edges.partitions.size
        g.partitionBy(PartitionStrategies.makeStrategy(tgp.pst, i, graphs.size, tgp.runs), numParts)
      }, storageLevel)
    } else
      this
  }

  override protected def fromDataFrames(vs: DataFrame, es: DataFrame, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false): SnapshotGraphWithSchema = SnapshotGraphWithSchema.fromDataFrames(vs, es, storLevel, coal)
  override protected def emptyGraph(sch: GraphSpec) = SnapshotGraphWithSchema.emptyGraph(sch)
}

object SnapshotGraphWithSchema {
  private val sqlContext = ProgramContext.getSqlContext
  import sqlContext.implicits._

  def fromDataFrames(vs: DataFrame, es: DataFrame, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): SnapshotGraphWithSchema = {
    val intervals = TGraphWithSchema.computeIntervals(vs, es)

    val graphs: ParSeq[Graph[Row,Row]] = intervals.map{ p =>
      val ps = Date.valueOf(p.start)
      val pe = Date.valueOf(p.end)
      Graph(vs.where(!($"estart" >= pe) || $"eend" <= ps).map(row => (row.getLong(0), row)), 
        es.where(!($"estart" >= pe || $"eend" <= ps)).map(row => Edge(row.getLong(0), row.getLong(1), row)), Row(), storLevel)
    }.par

    new SnapshotGraphWithSchema(intervals, vs, es, graphs, storLevel, coal = coalesced)

  }

  def emptyGraph(schema: GraphSpec):SnapshotGraphWithSchema = new SnapshotGraphWithSchema(Seq[Interval](), sqlContext.createDataFrame(ProgramContext.sc.emptyRDD[Row], StructType(schema.getVertexSchema)), sqlContext.createDataFrame(ProgramContext.sc.emptyRDD[Row], StructType(schema.getEdgeSchema)), ParSeq[Graph[Row,Row]](), coal = true)
}
