package edu.drexel.cs.dbgroup.temporalgraph.tools

import java.io.{File, FileWriter}
import java.sql.Date
import java.time.LocalDate

import _root_.edu.drexel.cs.dbgroup.temporalgraph.representations.SnapshotGraphParallel
import _root_.edu.drexel.cs.dbgroup.temporalgraph.tools.twitterToParquet.Nodes
import _root_.edu.drexel.cs.dbgroup.temporalgraph.{Interval, ProgramContext}
import _root_.edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
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
  val sc = new SparkContext(conf)
  ProgramContext.setContext(sc)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  case class Nodes(vid: Long, estart: Date, eend: Date, name:String)
  case class Edges(vid1: Long, vid2: Long,  estart: Date, eend: Date, count:Int)


  def main(args: Array[String]): Unit ={
    calculateGES("./sampleData", "sampleData.txt")
  }

  def calculateGES(source:String, destinationFileName:String): Unit ={
    GraphLoader.setGraphType("VE")
    val VE = GraphLoader.loadDataParquet(source)
    val intervals = VE.intervals.collect
    val edges = intervals.map(x => VE.getSnapshot(x.start).edges)
    val edgesCount = edges.map(x => x.count())
    val edgesAndCounts = edges.zip(edgesCount)
    val ges = edgesAndCounts.sliding(2).map(x =>
      if (x(0)._2 + x(1)._2 == 0){
        0
      }
      else{
        2 * x(0)._1.intersection(x(1)._1).count.toFloat / (x(0)._2 + x(1)._2)
      }
    ).toList

    //writing the results
    var f = new File("ges");
    f.mkdir()
    val fw = new FileWriter("ges/" + destinationFileName, true)
    for ( x <- ges) {
      fw.write(x.toString + "\n")
    }
    fw.write("Average = " + ges.sum/ges.size + "\n")
    fw.close()
  }
}
