package edu.drexel.cs.dbgroup.temporalgraph.tools

import java.sql.Date
import java.text.SimpleDateFormat
import java.time.LocalDate

import _root_.edu.drexel.cs.dbgroup.temporalgraph.{Interval, ProgramContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by shishir on 7/1/2016.
  */
object twitterToParquet {
  //note: this does not remove ALL logging
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  def dateOrdering: Ordering[LocalDate] = Ordering.fromLessThan((a,b) => a.isBefore(b))

  var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.network.timeout", "240")   
  conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", "500000")
  val sc = new SparkContext(conf)
  ProgramContext.setContext(sc)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  case class Nodes(vid: Long, estart: Date, eend: Date)
  case class Edges(vid1: Long, vid2: Long,  estart: Date, eend: Date)


  def main(args: Array[String]): Unit ={
    convertNodesAndEdges()
  }



  def convertNodesAndEdges(): Unit ={
    //Getting the nodes first
    val textFormat = new SimpleDateFormat("MMMyy");
    val finalFormat = new SimpleDateFormat("yyyy-MM-dd");
    val nodes = sc.textFile("hdfs://master:9000/data/twitter/account_creation_dates.csv").map(_.split(",")).map(x => (x(0), finalFormat.format(textFormat.parse(x(1)))))
    //val df1 = nodes.map(x => Nodes(x._1.toLong,  Date.valueOf(x._2), Date.valueOf("2013-01-01"))).toDF()
    //df1.printSchema()
    //df1.show()
    //df1.write.parquet("hdfs://master:9000/data/twitter/nodes.parquet")

    //Getting the edges using nodes
    val edgesLines = sc.textFile("hdfs://master:9000/data/twitter/followers_all.adj").map(line => (line.substring(0, line.indexOf(' ')), line.substring(line.indexOf(' ')+2).trim.split(" ")))
    val edgesWithoutDates = edgesLines.flatMap(x => (x._2.map(y => (x._1, y))))
    println("initial edges before join: " + edgesWithoutDates.count)
    val edges = edgesWithoutDates.join(nodes).map(x => ((x._2._1, ( x._1, x._2._2)))).join(nodes).map{x =>
      val firstDate = Date.valueOf(x._2._1._2)
      val secondDate = Date.valueOf(x._2._2)
      if (firstDate.after(secondDate)){
        ((x._2._1._1, x._1, firstDate, Date.valueOf("2013-01-01")))
      }
      else
        ((x._2._1._1, x._1, secondDate, Date.valueOf("2013-01-01")))
    }

    val df = edges.map(x => Edges(x._1.toLong,  x._2.toLong, x._3, x._4)).toDF()
    df.printSchema()
    df.show()
    df.write.parquet("hdfs://master:9000/data/twitter/edges.parquet")
    println("after join: " + df.count)
    println("DONE!")
  }




}
