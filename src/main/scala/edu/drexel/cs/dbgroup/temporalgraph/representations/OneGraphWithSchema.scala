package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.reflect.ClassTag
import scala.util.control._
import scala.language.implicitConversions
import scala.collection.immutable.BitSet
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
import org.apache.spark.sql.types.{StructType,StructField,IntegerType,DateType}

import org.apache.spark.graphx.impl.GraphXPartitionExtension._
import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps._

import java.time.LocalDate
import java.sql.Date

class OneGraphWithSchema(intvs: Seq[Interval], verts: DataFrame, edgs: DataFrame, grs: Graph[BitSet, BitSet], storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraphWithSchema(intvs, verts, edgs, storLevel, coal) {
  import sqlContext.implicits._

  private val graphs: Graph[BitSet, BitSet] = grs

  override def materialize() = {
    allVertices.count
    allEdges.count
    graphs.numVertices
    graphs.numEdges
  }

  /** Query operations */
  override def slice(bound: Interval): OneGraphWithSchema = {
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

      val sb = Date.valueOf(startBound)
      val eb = Date.valueOf(endBound)
      val mnf: (Date => Date) = (in: Date) => if (in.before(eb)) in else eb
      val mxf: (Date => Date) = (in: Date) => if (sb.before(in)) in else sb
      val maxFunc = udf(mxf)
      val minFunc = udf(mnf)
      val clsv = Array(allVertices("vid"), maxFunc($"estart").as("estart"), minFunc($"eend").as("eend")) ++ allVertices.columns.drop(3).map(c => allVertices(c))
      val clse = Array(allEdges("vid1"), allEdges("vid2"), maxFunc($"estart").as("estart"), minFunc($"eend").as("eend")) ++ allEdges.columns.drop(4).map(c => allEdges(c))

      val vattrs = allVertices.where(!($"estart" >= eb || $"eend" <= sb)).select(clsv: _*)
      val eattrs = allEdges.where(!($"estart" >= eb || $"eend" <= sb)).select(clse: _*)

      new OneGraphWithSchema(newIntvs, vattrs, eattrs, resg, storageLevel)

    } else
      OneGraphWithSchema.emptyGraph(schema)
  }

  override def pregel[A: ClassTag]
     (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, Row, A) => Row,
       sendMsg: EdgeTriplet[Row, Row] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): OneGraphWithSchema = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def degree: RDD[(VertexId, (Interval, Int))] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    if (graphs.edges.isEmpty)
      0
    else
      graphs.edges.partitions.size
  }

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): OneGraphWithSchema = {
    super.persist(newLevel)
    graphs.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): OneGraphWithSchema = {
    super.unpersist(blocking)
    graphs.unpersist(blocking)
    this
  }

  override def partitionBy(tgp: TGraphPartitioning): OneGraphWithSchema = {
    var numParts = if (tgp.parts > 0) tgp.parts else graphs.edges.partitions.size

    if (tgp.pst != PartitionStrategyType.None) {
      //not changing the intervals
      new OneGraphWithSchema(intervals, allVertices, allEdges, graphs.partitionByExt(PartitionStrategies.makeStrategy(tgp.pst, 0, intervals.size, tgp.runs), numParts), storageLevel)
    } else
      this
  }

  override protected def fromDataFrames(vs: DataFrame, es: DataFrame, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false): OneGraphWithSchema = OneGraphWithSchema.fromDataFrames(vs, es, storLevel, coal)
  override protected def emptyGraph(sch: GraphSpec) = OneGraphWithSchema.emptyGraph(sch)  

}

object OneGraphWithSchema {
  private val sqlContext = ProgramContext.getSqlContext
  import sqlContext.implicits._

  def fromDataFrames(vs: DataFrame, es: DataFrame, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): OneGraphWithSchema = {
    val intervals = TGraphWithSchema.computeIntervals(vs, es)
    val intvs = sqlContext.createDataFrame(ProgramContext.sc.parallelize(intervals.zipWithIndex.map(x => Row(x._2, Date.valueOf(x._1.start), Date.valueOf(x._1.end)))), StructType(StructField("index", IntegerType, false) :: StructField("start", DateType, false) :: StructField("end", DateType, false) :: Nil))

    //TODO: can rewrite with collect_set if hive is enabled
    val verts = vs.select($"vid", $"estart", $"eend").join(intvs).where(!($"estart" >= $"end" || $"eend" <= $"start")).select($"vid", $"index").rdd.map(row => (row.getLong(0), BitSet(row.getInt(1)))).reduceByKey((a,b) => a union b)
    val edgs = es.select($"vid1", $"vid2", $"estart", $"eend").join(intvs).where(!($"estart" >= $"end" || $"eend" <= $"start")).select($"vid1", $"vid2", $"index").rdd.map(row => ((row.getLong(0), row.getLong(1)), BitSet(row.getInt(2)))).reduceByKey((a,b) => a union b).map(e => Edge(e._1._1, e._1._2, e._2))

    val graphs: Graph[BitSet, BitSet] = Graph(verts, edgs, BitSet(), storLevel)

    new OneGraphWithSchema(intervals, vs, es, graphs, storLevel, coalesced)
  }

  def emptyGraph(schema: GraphSpec):OneGraphWithSchema = new OneGraphWithSchema(Seq[Interval](), sqlContext.createDataFrame(ProgramContext.sc.emptyRDD[Row], StructType(schema.getVertexSchema)), sqlContext.createDataFrame(ProgramContext.sc.emptyRDD[Row], StructType(schema.getEdgeSchema)), Graph[BitSet,BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD), coal = true)
}
