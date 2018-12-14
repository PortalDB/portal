package edu.drexel.cs.dbgroup.portal.tools

import java.sql.Date
import java.time.LocalDate

import edu.drexel.cs.dbgroup.portal.util.MultifileLoad
import edu.drexel.cs.dbgroup.portal.{Interval, ProgramContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * Created by shishir on 5/9/2016.
  */
object UKDelisParquet {

  //note: this does not remove ALL logging
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  implicit def dateOrdering: Ordering[LocalDate] = Ordering.fromLessThan((a,b) => a.isBefore(b))


  var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)
  ProgramContext.setContext(sc)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  case class Nodes(vid: Long, estart: Date, eend: Date, name: String)
  case class Edges(vid1: Long, vid2: Long,  estart: Date, eend: Date)

  def main(args: Array[String]) {
    convertNodesHDFS()
    convertEdgesHDFS()
  }

  def convertNodesHDFS(): Unit ={
    var nodes: RDD[(VertexId, (Interval, String))] = MultifileLoad.readNodes("./ukdelis/", LocalDate.parse("2006-01-01"), LocalDate.parse("2007-12-01")).flatMap{ x =>
      val (filename, line) = x
      val start = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))
      val parts = line.split(",")

      if (parts.size > 1 && parts.head != "") {
        Some((parts.head.toLong, (Interval(start,start.plusMonths(1)), parts(1))))
      } else None
    }

    val coalesced = coalesce(nodes)
    val df = coalesced.map(x => Nodes(x._1,  Date.valueOf(x._2._1.start), Date.valueOf(x._2._1.end), x._2._2)).toDF()
    df.printSchema()
    df.show()
    df.write.parquet("./ukdelis/nodes.parquet")
  }

  def convertEdgesHDFS(): Unit ={
    var edges: RDD[((Long, Long), (Interval, Int))] = MultifileLoad.readEdges("./ukdelis/", LocalDate.parse("2006-01-01"), LocalDate.parse("2007-12-01")).flatMap{ x =>
      val (filename, line) = x
      val start = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))

      val parts = line.split("\\s+")
      if (parts.size > 1 && parts.head != "") {
        Some(((parts.head.toLong, parts(1).toLong), (Interval(start,start.plusMonths(1)), 1)))
      } else None
    }

    val coalesced = coalesce(edges)
    val df = coalesced.map(x => Edges(x._1._1,  x._1._2, Date.valueOf(x._2._1.start), Date.valueOf(x._2._1.end))).toDF()
    df.printSchema()
    df.show()
    df.write.parquet("./ukdelis/edges.parquet")
  }

  def coalesce[K: ClassTag, V: ClassTag](rdd: RDD[(K, (Interval, V))]): RDD[(K, (Interval, V))] = {
    rdd.groupByKey.mapValues{ seq =>  //groupbykey produces RDD[(K, Seq[(p, V)])]
      seq.toSeq.sortBy(x => x._1.start)
        .foldLeft(List[(Interval, V)]()){ (r,c) => r match {
          case head :: tail =>
            if (head._2 == c._2 && head._1.end == c._1.start) (Interval(head._1.start, c._1.end), head._2) :: tail
            else c :: head :: tail
          case Nil => List(c)
        }
        }}.flatMap{ case (k,v) => v.map(x => (k, x))}
  }
}
