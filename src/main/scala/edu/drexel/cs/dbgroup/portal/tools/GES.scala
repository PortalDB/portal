package edu.drexel.cs.dbgroup.portal.tools

import java.io.{File, FileWriter}
import java.sql.Date
import java.time.LocalDate
import scala.collection.mutable.ListBuffer

import _root_.edu.drexel.cs.dbgroup.portal.representations.RepresentativeGraph
import _root_.edu.drexel.cs.dbgroup.portal.tools.twitterToParquet.Nodes
import _root_.edu.drexel.cs.dbgroup.portal.{StructureOnlyAttr, Interval, ProgramContext, EdgeId}
import _root_.edu.drexel.cs.dbgroup.portal.util.GraphLoader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{EdgeRDD, Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by shishir on 7/14/2016.
  */
object GES {
  //note: this does not remove ALL logging
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.network.timeout", "240")
  //conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", "500000")
  val sc = new SparkContext(conf)
  ProgramContext.setContext(sc)
  val sqlContext = ProgramContext.getSession
  //TODO: remove hard-coding of this parameter. currently it is 1024x1024x16, i.e. 16mb
  sqlContext.conf.set("spark.sql.files.maxPartitionBytes", "16777216")
  import sqlContext.implicits._
  case class Nodes(vid: Long, estart: Date, eend: Date, name:String)
  case class Edges(vid1: Long, vid2: Long,  estart: Date, eend: Date, count:Int)


  def main(args: Array[String]): Unit ={
    calculateGES("hdfs://master:9000/data/twitter", "twitter.txt")
  }
  def calculateGES(source:String, destinationFileName:String): Unit ={
    var f = new File("ges");
    f.mkdir()
    val fw = new FileWriter("ges/" + destinationFileName, true)
    val VE = GraphLoader.buildVE(source, -1, -1, Interval(LocalDate.MIN, LocalDate.MAX))
    var intervals = VE.getTemporalSequence.collect
    intervals = intervals.drop(69)
    println(intervals.size)
    intervals.foreach(println)
    var edges = new ListBuffer[EdgeRDD[(EdgeId, Any)]]()
    val edgesCount = new ListBuffer[Long]()
    var edge = VE.getSnapshot(intervals(0).start).edges
    edges += edge
    edgesCount += edge.count()
    print(edgesCount(0))
    var sum:Float = 0
    var count = 0;
    for (i <- 0 to (intervals.size - 2)) {
      edge = VE.getSnapshot(intervals(i+1).start).edges
      edges += edge
      edgesCount += edge.count()
      print(edgesCount(i))
      var ges:Float = 0
      if (edgesCount(i) + edgesCount(i+1) == 0){
        ges = 0
      }
      else {
        ges = (2 * edges(i).intersection(edges(i+1)).count.toFloat) / (edgesCount(i) + edgesCount(i+1))
      }

      println(intervals(i).toString() + " and " + intervals(i+1).toString() + " =" + ges.toString)
      fw.write(intervals(i).toString() + " and " + intervals(i+1).toString() + " =" + " " + ges.toString + "\n")
      sum = sum + ges
      count = count + 1
    }
//    fw.write("Average = " + sum/count + "\n")
    fw.close()
  }
} 
