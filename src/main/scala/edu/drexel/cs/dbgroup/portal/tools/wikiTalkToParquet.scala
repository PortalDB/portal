package edu.drexel.cs.dbgroup.portal.tools

/**
  * Created by shishir on 6/21/2016.
  */
import java.io.FileWriter
import java.sql.Date
import java.time.LocalDate

import edu.drexel.cs.dbgroup.portal.{Interval, ProgramContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object wikiTalkToParquet{

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
    convertNodes()
    convertEdges()
  }


  def convertNodes(): Unit ={
    val vidAttributes: RDD[(String, (String, String, String, Array[String]))] = sc.textFile("wiki-talk-dataset/data/en-user-info").map(_.split("\t")).map(x => (x(0), (x(1), x(2), x(3), x(4).split(","))))
    val edges = sc.textFile("wiki-talk-dataset/data/en-wiki-talk").map(_.split("\t"))
    val allVids: RDD[String] = edges.flatMap(x => List(x(0), x(1))).distinct()
    val userjoinRDD: RDD[(String, Option[(String,String,String,Array[String])])] = allVids.map(x => (x, null)).leftOuterJoin(vidAttributes).mapValues(x => x._2)

    //If the vid does not exist in api, give it dafault values
    val vidJoined: RDD[(String, (String,String,String,Array[String]))] = userjoinRDD.map { case (a, b: Option[(String, String, String, Array[String])]) => (a, b.getOrElse(("", "Default", "0", Array[String]()))) }

    //Check for the all vids without registration date and add the earliest date from edges
    val vidWithoutRegistration = vidJoined.filter(x => x._2._1 == "")
    val vidWithRegistration = vidJoined.filter(x => x._2._1 != "")
    println("without registration count=" + vidWithoutRegistration.count)
    val verticesWithEdgeDates: RDD[(String, String)] = edges.flatMap(x => List((x(0), x(2)), (x(1), x(2))))
    val vidGrouped: RDD[(String, (Iterable[(String, String, String, Array[String])], Iterable[String]))] = vidWithoutRegistration.cogroup(verticesWithEdgeDates)
    val vidGrouped2: RDD[(String, (String, String, String, Array[String]))] = vidGrouped.flatMap{ case (k, (v1, v2)) => if (v1.size < 1) None else if (v2.size > 0) Some(k, (v1.toList.head.copy(_1 = v2.min))) else None}
    println("without registration updated count=" + vidGrouped2.count())

    val nodes = vidWithRegistration.union(vidGrouped2)
    val df = nodes.map(x => Nodes(x._1.toLong,  Date.valueOf(x._2._1.toString.split("T")(0)), Date.valueOf("2016-06-30"), x._2._2, x._2._3.toInt, x._2._4)).toDF()
    df.printSchema()
    df.show()
    df.write.parquet("wikitalk/nodes.parquet")
  }

  def convertEdges(): Unit ={
    var lines = sc.textFile("wiki-talk-dataset/data/en-wiki-talk")
    val edges = lines.map(_.split("\t")).map(x => ((x(0).toLong, x(1).toLong), (Interval(LocalDate.parse(x(2).split("T")(0)), LocalDate.parse(x(2).split("T")(0)).plusDays(1)), 1)))
    println("edges before", edges.count)
    val coalesced = coalesce(edges)
    println("edges after", coalesced.count)
    val df = coalesced.map(x => Edges(x._1._1,  x._1._2, Date.valueOf(x._2._1.start), Date.valueOf(x._2._1.end))).toDF()
    df.printSchema()
    df.show()
    df.write.parquet("wikitalk/edges.parquet")
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