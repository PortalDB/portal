package edu.drexel.cs.dbgroup.temporalgraph

import scala.reflect.ClassTag
import java.time.LocalDate
import java.sql.Date

import org.apache.spark.sql.types.{StructType,StructField,LongType,DateType}
//import org.apache.spark.sql.catalyst.expressions.NamedExpression
//import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.{DataFrame,Row,Column}
import org.apache.spark.graphx.{Graph,Edge,VertexId}
import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.rdd.RDDFunctions._

import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps._

abstract class TGraphWithSchema(intvs: Seq[Interval], verts: DataFrame, edgs: DataFrame, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraph[Row,Row] {
  protected val sqlContext = ProgramContext.getSqlContext
  import sqlContext.implicits._

  //TODO: remove hard-coded column names like estart and eend and vid from code
  //replace with variables

  //the vertices data frame has the schema vid, start, end, and then as many attributes as there are
  val allVertices: DataFrame = verts
  //the edges data frame has the schema vid1, vid2, start, end, and then as many attributes as there are
  val allEdges: DataFrame = edgs
  val intervals: Seq[Interval] = intvs
  val storageLevel = storLevel
  val coalesced = coal

  lazy val span: Interval = if (intervals.size > 0) Interval(intervals.head.start, intervals.last.end) else Interval(LocalDate.now, LocalDate.now)

  lazy val schema: GraphSpec = {
    GraphSpec(allVertices.schema.fields, allEdges.schema.fields)
  }

  override def size(): Interval = span

  override def vertices: RDD[(VertexId, (Interval, Row))] = {
    //the row in the result is just the attributes
    allVertices.map{ r =>
      (r.getLong(0), (Interval(r.getDate(1), r.getDate(2)), Row.fromSeq(r.toSeq.drop(3))))
    }
  }

  override def verticesAggregated: RDD[(VertexId, Map[Interval, Row])] = {
    allVertices.map{ r =>
      (r.getLong(0), Map[Interval, Row](Interval(r.getDate(1), r.getDate(2)) -> Row.fromSeq(r.toSeq.drop(3))))
    }.reduceByKey((a, b) => a ++ b)
  }

  override def edges: RDD[((VertexId, VertexId), (Interval, Row))] = {
    allEdges.map{ r =>
      ((r.getLong(0), r.getLong(1)), (Interval(r.getDate(2), r.getDate(3)), Row.fromSeq(r.toSeq.drop(4))))
    }
  }

  override def edgesAggregated: RDD[((VertexId, VertexId), Map[Interval, Row])] = {
    allEdges.map{ r =>
      ((r.getLong(0), r.getLong(1)), Map[Interval, Row](Interval(r.getDate(2), r.getDate(3)) -> Row.fromSeq(r.toSeq.drop(4))))
    }.reduceByKey((a, b) => a ++ b)
  }

  def verticesDataFrame: DataFrame = allVertices
  def edgesDataFrame: DataFrame = allEdges

  override def getTemporalSequence: Seq[Interval] = intervals

  override def getSnapshot(time: LocalDate): Graph[Row, Row] = {
    val in = Date.valueOf(time)
    Graph[Row, Row](allVertices.where($"estart" <= in).where($"eend" > in).map(r => (r.getLong(0), Row.fromSeq(r.toSeq.drop(3)))),
      allEdges.where($"estart" <= in).where($"eend" > in).map(r => Edge(r.getLong(0), r.getLong(1), Row.fromSeq(r.toSeq.drop(4)))), Row(), storageLevel)
  }

  override def coalesce(): TGraphWithSchema = {
    //coalesce the vertices and edges
    //then recompute the intervals and graphs
    if (coalesced)
      this
    else
      fromDataFrames(TGraphWithSchema.coalesceV(allVertices), TGraphWithSchema.coalesceE(allEdges), storageLevel, coal = true)
  }

  override def slice(bound: Interval): TGraphWithSchema = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this

    if (!span.intersects(bound)) {
      return emptyGraph(schema)
    }

    val startBound: Date = if (bound.start.isAfter(span.start)) bound.start else span.start
    val endBound: Date = if (bound.end.isBefore(span.end)) bound.end else span.end

    val mnf: (Date => Date) = (in: Date) => if (in.before(endBound)) in else endBound
    val mxf: (Date => Date) = (in: Date) => if (startBound.before(in)) in else startBound
    val maxFunc = udf(mxf)
    val minFunc = udf(mnf)
    val clsv = Array(allVertices("vid"), maxFunc($"estart").as("estart"), minFunc($"eend").as("eend")) ++ allVertices.columns.drop(3).map(c => allVertices(c))
    val clse = Array(allEdges("vid1"), allEdges("vid2"), maxFunc($"estart").as("estart"), minFunc($"eend").as("eend")) ++ allEdges.columns.drop(4).map(c => allEdges(c))

    fromDataFrames(allVertices.where(!($"estart" >= endBound || $"eend" <= startBound)).select(clsv: _*), allEdges.where(!($"estart" >= endBound || $"eend" <= startBound)).select(clse: _*), storageLevel)
  }

  def select(vtpred: Interval => Boolean, etpred: Interval => Boolean): TGraphWithSchema = {
    //the easiest way to enforce the integrity constraint is to apply both vtpred and etpred on the edges
    val sfv: (Date, Date) => Boolean = (in1: Date, in2: Date) => vtpred(Interval(in1, in2))
    val selectFuncV = udf(sfv)
    val sfe: (Date, Date) => Boolean = (in1: Date, in2: Date) => { val intv = Interval(in1, in2); vtpred(intv) && etpred(intv) }
    val selectFuncE = udf(sfe)
    fromDataFrames(allVertices.where(selectFuncV($"estart", $"eend")), allEdges.where(selectFuncE($"estart", $"eend")))
  }

  /**
    * Restrict the graph to only the vertices and edges that satisfy the predicates.
    * @param epred The edge predicate, which takes an edge and evaluates to true 
    * if the edge is to be included.
    * @param vpred The vertex predicate, which takes a vertex object and evaluates 
    * to true if the vertex is to be included.
    * This is the most general version of select.
    * @return The temporal subgraph containing only the vertices and edges 
    * that satisfy the predicates. The result is coalesced which
    * may cause different representative intervals.
    */
  def select(vpred: Option[Column] = None, epred: Option[Column] = None): TGraphWithSchema = {
    val newVerts: DataFrame = if (vpred.isEmpty) allVertices else allVertices.where(vpred.get)
    val filteredEdges = if (epred.isEmpty) allEdges else allEdges.where(epred.get)

    val newEdges = if (vpred.isEmpty) filteredEdges else TGraphWithSchema.constrainEdges(newVerts, filteredEdges)
    fromDataFrames(newVerts, newEdges)
  }

  protected val vgb = (vid: VertexId, attr: Row) => vid
  def aggregate(window: WindowSpecification, vquant: Quantification, equant: Quantification, vAggFunc: (Row, Row) => Row, eAggFunc: (Row, Row) => Row)(vgroupby: (VertexId, Row) => VertexId = vgb): TGraphWithSchema = {
    if (allVertices.rdd.isEmpty)
      return emptyGraph(schema)

    window match {
      case c : ChangeSpec => aggregateByChange(c, vgroupby, vquant, equant, vAggFunc, eAggFunc)
      case t : TimeSpec => aggregateByTime(t, vgroupby, vquant, equant, vAggFunc, eAggFunc)
      case _ => throw new IllegalArgumentException("unsupported window specification")
    }
  }

  protected def aggregateByChange(c: ChangeSpec, vgroupby: (VertexId, Row) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (Row, Row) => Row, eAggFunc: (Row, Row) => Row): TGraphWithSchema = {
    val size: Integer = c.num
/*
    val locali = intervals.sliding(size)
    val split: (Interval => Iterator[(Date, Date, List[(Date, Date)])]) = (interval: Interval) => {
      locali.flatMap{ group: Seq[Interval] =>
        val res = group.flatMap{ case intv =>
          if (intv.intersects(interval)) Some(Date.valueOf(intv.start), Date.valueOf(intv.end)) else None
        }.toList
        if (res.isEmpty)
          None
        else
          Some(Date.valueOf(group.head.start), Date.valueOf(group.last.end), res)
      }
    }
    val gb = (cols: Seq[Any]) => vgroupby(cols(0).asInstanceOf[Long], Row(cols))
    val groupby = udf(gb)
    
    val cols: Array[Column] = Array(groupby(allVertices.columns.map(cl => allVertices(cl)): _*) as "newvid") ++ allVertices.columns.drop(1).map(c => allVertices(c))
    val newidsv = if (vgroupby == vgb) allVertices else allVertices.select(cols: _*)
    val splitVerts = newidsv.explode($"estart", $"eend") { row =>
      //makes multiple rows with 3 new columns _1, _2, _3
      split(Interval(row.getDate(0), row.getDate(1))).map(ii => (ii._1, ii._2, ii._3))
    }

    val newidse = if (vgroupby == vgb) allEdges else {
      //we need to join e with v, twice, to compute new vid1 vid2
      val mnf: (Date, Date, Date) => Date = (in1: Date, in2: Date, in3: Date) => {
        if (in1.before(in2)) {
          if (in1.before(in3)) in1 else in3
        } else if (in2.before(in3)) in2 else in3
      }
      val mxf: (Date, Date, Date) => Date = (in1: Date, in2: Date, in3: Date) => {
        if (in1.after(in2)) {
          if (in1.after(in3)) in1 else in3
        } else if (in2.after(in3)) in2 else in3
      }
      val maxFunc = udf(mxf)
      val minFunc = udf(mnf)

      val vids = newidsv.select($"vid", $"newvid")
      val ecols: Array[Column] = Array($"v1.newvid" as "vid1", $"v2.newvid" as "vid2", maxFunc($"e.estart", $"v1.estart", $"v2.estart") as "estart", minFunc($"e.eend", $"v1.eend", $"v2.eend") as "eend") ++ allEdges.columns.drop(4).map(c => allEdges(c))
      allEdges.as("e").join(vids.as("v1"), $"e.vid1" === $"v1.vid")
        .where(!($"e.estart" >= $"v1.eend" || $"e.eend" <= $"v1.estart"))
        .join(vids.as("v2"), $"e.vid2" === $"v2.vid")
        .where(!($"e.estart" >= $"v2.eend" || $"e.eend" <= $"v2.estart"))
        .select(ecols: _*)
    }
    val splitEdges = newidse.explode($"estart", $"eend") { row =>
      split(Interval(row.getDate(0), row.getDate(1))).map(ii => (ii._1, ii._2, ii._3))
    }

    //now that we have a row for each interval, we can do group by
 */
    throw new UnsupportedOperationException("not yet implemented")
  }

  protected def aggregateByTime(c: TimeSpec, vgroupby: (VertexId, Row) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (Row, Row) => Row, eAggFunc: (Row, Row) => Row): TGraphWithSchema = {
    throw new UnsupportedOperationException("not yet implemented")
  }

/*


  def projectVertices(input: Seq[NamedExpression]): TemporalGraphWithSchema
  def projectEdges(input: Seq[NamedExpression]): TemporalGraphWithSchema

  //TODO: add the others as necessary
  //FIXME: figure out a way to not have to list all methods inherited from
  //parent and still return this type (use member type?)
  def mapVerticesWIndex(map: (VertexId, TimeIndex, InternalRow) => InternalRow, newSchema: GraphSpec): TemporalGraphWithSchema
  def mapEdgesWIndex(map: (Edge[InternalRow], TimeIndex) => InternalRow, newSchema: GraphSpec): TemporalGraphWithSchema
  def aggregate(res: Resolution, vsem: AggregateSemantics.Value, esem: AggregateSemantics.Value, vAggFunc: (InternalRow, InternalRow) => InternalRow, eAggFunc: (InternalRow, InternalRow) => InternalRow): TemporalGraphWithSchema
 */
  override def persist(newLevel: StorageLevel = MEMORY_ONLY): TGraphWithSchema = {
    allVertices.persist(newLevel)
    allEdges.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): TGraphWithSchema = {
    allVertices.unpersist(blocking)
    allEdges.unpersist(blocking)
    this
  }

  protected def fromDataFrames(vs: DataFrame, es: DataFrame, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false): TGraphWithSchema
  protected def emptyGraph(sch: GraphSpec): TGraphWithSchema
}

object TGraphWithSchema {
  val sqlContext = ProgramContext.getSqlContext
  import sqlContext.implicits._

  def computeIntervals(verts: DataFrame, edgs: DataFrame): Seq[Interval] = {
    verts.select($"estart" as "dt").unionAll(verts.select($"eend") as "dt").unionAll(edgs.select($"estart") as "dt").unionAll(edgs.select($"eend") as "dt").distinct().sort("dt").rdd.sliding(2).map(lst => Interval(lst(0).getDate(0).toLocalDate(), lst(1).getDate(0).toLocalDate())).collect()
  }

  /*
   * Given an dataframe of vertices and an dataframe of edges,
   * remove the edges for which there is no src or dst vertex
   * at the indicated time period,
   * shorten the periods as needed to meet the integrity constraint.
   * Warning: This is a very expensive operation, use only when needed.
   */
  def constrainEdges(verts: DataFrame, edgs: DataFrame): DataFrame = {
    val coalesceV = TGraphWithSchema.coalesceStructureV(verts)

    val mnf: (Date, Date, Date) => Date = (in1: Date, in2: Date, in3: Date) => {
      if (in1.before(in2)) {
        if (in1.before(in3)) in1 else in3
      } else if (in2.before(in3)) in2 else in3
    }
    val mxf: (Date, Date, Date) => Date = (in1: Date, in2: Date, in3: Date) => {
      if (in1.after(in2)) {
        if (in1.after(in3)) in1 else in3
      } else if (in2.after(in3)) in2 else in3
    }
    val maxFunc = udf(mxf)
    val minFunc = udf(mnf)

    val cols = Array($"e.vid1", $"e.vid2", maxFunc($"e.estart", $"v1.estart", $"v2.estart").as("estart"), minFunc($"e.eend", $"v1.eend", $"v2.eend").as("eend")) ++ edgs.columns.drop(4).map(c => edgs(c))
    //join with vertices based on ids and periods
    edgs.as("e").join(coalesceV.as("v1"), $"e.vid1" === $"v1.vid")
      .where(!($"e.estart" >= $"v1.eend" || $"e.eend" <= $"v1.estart"))
      .join(coalesceV.as("v2"), $"e.vid2" === $"v2.vid")
      .where(!($"e.estart" >= $"v2.eend" || $"e.eend" <= $"v2.estart"))
      .select(cols: _*)

  }

  //TODO: find a better way to coalesce that doesn't require separate functions
  //for V and E
  def coalesceV(table: DataFrame): DataFrame = {
    //TODO - when SparkSQL adds support for NOT EXISTS, rewrite
    //using SparkSQL
    implicit val ord = TempGraphOps.dateOrdering
    sqlContext.createDataFrame(table.map(r => (r.getLong(0), (Interval(r.getDate(1), r.getDate(2)), r.toSeq.drop(3))))
      .groupByKey
      .mapValues { seq =>
      seq.toSeq.sortBy(x => x._1.start)
      .foldLeft(List[(Interval, Seq[Any])]()){ (r,c) => r match {
        case head :: tail => 
          if (head._1.end == c._1.start && head._2 == c._2) (Interval(head._1.start, c._1.end), head._2) :: tail
          else c :: head :: tail
        case Nil => List(c)
      }
      }}.flatMap{ case (k,v) => v.map(x => Row.fromSeq(Seq(k, x._1.start, x._1.end) ++ x._2))}, table.schema)
  }

  def coalesceE(table: DataFrame): DataFrame = {
    //TODO - when SparkSQL adds support for NOT EXISTS, rewrite
    //using SparkSQL
    implicit val ord = TempGraphOps.dateOrdering
    sqlContext.createDataFrame(table.map(r => ((r.getLong(0), r.getLong(1)), (Interval(r.getDate(2), r.getDate(3)), r.toSeq.drop(4))))
      .groupByKey
      .mapValues { seq =>
      seq.toSeq.sortBy(x => x._1.start)
      .foldLeft(List[(Interval, Seq[Any])]()){ (r,c) => r match {
        case head :: tail => 
          if (head._1.end == c._1.start && head._2 == c._2) (Interval(head._1.start, c._1.end), head._2) :: tail
          else c :: head :: tail
        case Nil => List(c)
      }
      }}.flatMap{ case (k,v) => v.map(x => Row.fromSeq(Seq(k._1, k._2, x._1.start, x._1.end) ++ x._2))}, table.schema)
  }

  def coalesceStructureV(table: DataFrame): DataFrame = {
    //TODO - when SparkSQL adds support for NOT EXISTS, rewrite
    //using SparkSQL
    implicit val ord = TempGraphOps.dateOrdering
    sqlContext.createDataFrame(table.map(r => (r.getLong(0), Interval(r.getDate(1), r.getDate(2))))
      .groupByKey
      .mapValues{ seq =>
      seq.toSeq.sortBy(x => x.start)
      .foldLeft(List[Interval]()){ (r,c) => r match {
        case head :: tail => if (head.end == c.start) Interval(head.start, c.end) :: tail else c :: r
        case Nil => List(c)
      }}
    }.flatMap{ case (k,v) => v.map(x => Row(k, x.start, x.end))}, StructType(StructField("vid", LongType, false) :: StructField("estart", DateType, false) :: StructField("eend", DateType, false) :: Nil))
  }
    
}
