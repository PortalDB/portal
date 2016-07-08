package edu.drexel.cs.dbgroup.temporalgraph.tools

import java.sql.Date
import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph.tools.wikiTalkToParquet.Edges
import edu.drexel.cs.dbgroup.temporalgraph.{Interval, ProgramContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.reflect.ClassTag


object wikiTalkEdgesToMonths {
  //note: this does not remove ALL logging
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  def dateOrdering: Ordering[LocalDate] = Ordering.fromLessThan((a,b) => a.isBefore(b))

  var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)
  ProgramContext.setContext(sc)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  case class Nodes(vid: Long, estart: Date, eend: Date, name:String, editCount:Int, groups:Array[String])
  case class Edges(vid1: Long, vid2: Long,  estart: Date, eend: Date)

  def main(args: Array[String]): Unit ={
    convertEdgesToMonths()
  }

  def convertEdgesToMonths(): Unit ={
    var wikitalkEdges = sqlContext.read.parquet("wikitalk/edges.parquet")
    val edgesNotDistinct = wikitalkEdges.rdd.map{x =>
      val initialStart = x(2).asInstanceOf[Date].toLocalDate
      val startDate = initialStart.minusDays(initialStart.getDayOfMonth - 1)
      val endDate = startDate.plusMonths(1)
      ((x(0).asInstanceOf[Long], x(1).asInstanceOf[Long]), (Interval(startDate, endDate), 1))
    }
    println("edges before distinct", edgesNotDistinct.count)
    val edges = edgesNotDistinct.distinct()

    println("edges after distinct", edges.count)
    val coalesced = coalesce(edges)
    println("edges after coalesce", coalesced.count)
    val df = coalesced.map(x => Edges(x._1._1,  x._1._2, Date.valueOf(x._2._1.start), Date.valueOf(x._2._1.end))).toDF()
    df.printSchema()
    df.show()
    df.write.parquet("wikitalk/edgesWithMonths.parquet")
  }

  def coalesce[K: ClassTag, V: ClassTag](rdd: RDD[(K, (Interval, V))]): RDD[(K, (Interval, V))] = {
    implicit val ord = dateOrdering
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
