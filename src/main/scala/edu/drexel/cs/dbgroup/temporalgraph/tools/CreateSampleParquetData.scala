package edu.drexel.cs.dbgroup.temporalgraph.tools

import java.io.File
import java.sql.Date
import java.time.LocalDate

import _root_.edu.drexel.cs.dbgroup.temporalgraph.tools.GES.{Edges, Nodes}
import _root_.edu.drexel.cs.dbgroup.temporalgraph.{ProgramContext, Interval}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


/**
  * Created by shishir on 7/14/2016.
  */
object CreateSampleParquetData {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)
  ProgramContext.setContext(sc)
  val sqlContext = ProgramContext.getSession
  sqlContext.conf.set("spark.sql.files.maxPartitionBytes", "16777216")
  import sqlContext.implicits._
  case class Nodes(vid: Long, estart: Date, eend: Date, name:String)
  case class Edges(vid1: Long, vid2: Long,  estart: Date, eend: Date, count:Int)

  def main(args: Array[String]): Unit ={
    createSampleDataParquet()
  }

  def createSampleDataParquet() = {
    val nodes: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 21)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 25)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 12)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 92))
    ))

    val df1 = nodes.map(x => Nodes(x._1.toLong,  Date.valueOf(x._2._1.start), Date.valueOf(x._2._1.end), x._2._2)).toDF()
    val df2 = edges.map(x => Edges(x._1._1.toLong, x._1._2.toLong, Date.valueOf(x._2._1.start), Date.valueOf(x._2._1.end), x._2._2)).toDF()


    var f = new File("/home/sk3432/temporaldata/sampleData");
    f.mkdir()

    df1.write.parquet("/home/sk3432/temporaldata/sampleData/nodes.parquet")
    df2.write.parquet("/home/sk3432/temporaldata/sampleData/edges.parquet")

  }
}
