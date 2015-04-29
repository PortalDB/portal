package edu.drexel.cs.dbgroup.graphxt

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph
import scala.util.control._
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx.impl.GraphXPartitionExtension._

object Driver {
  def main(args: Array[String]) = {

    //note: this does not remove ALL logging  
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    var graphType: String = "SG"
    var strategy: String = ""
    var iterations: Int = 1
    var data = ""
    var partitionType: PartitionStrategyType.Value = PartitionStrategyType.None
    var numParts: Int = -1
    var warmStart = false

    for (i <- 0 until args.length) {
      if (args(i) == "--type") {
        graphType = args(i + 1)
        if (graphType == "MG")
          println("Running experiments with MultiGraph")
        else if (graphType == "SG")
          println("Running experiments with SnapshotGraph")
        else if (graphType == "SGP")
          println("Running experiments with parallel SnapshotGraph")
        else {
          println("Invalid graph type, exiting")
          System.exit(1)
        }
      } else if (args(i) == "--strategy") {
        strategy = args(i + 1)
      } else if (args(i) == "--iterations") {
        iterations = args(i + 1).toInt
      } else if (args(i) == "--data") {
        data = args(i + 1)
      } else if (args(i) == "--warmstart") {
        warmStart = true
      }
    }

    //For local spark execution uncomment these 3 lines
    //val conf = new SparkConf().setMaster("local").setAppName("TemporalGraph Project")
    //       .setSparkHome(System.getenv("SPARK_HOME")).
    //	   .setJars(List("target/scala-2.10/temporal-graph-project_2.10-1.0.jar","lib/graphx-extensions_2.10-1.0.jar"))

    //For amazon ec2 execution uncomment these 4 lines. Make sure to use the correct spark master uri
    //val conf = new SparkConf().setMaster("spark://ec2-54-234-129-137.compute-1.amazonaws.com:7077")
    //       .setAppName("TemporalGraph Project")
    //	   .setSparkHome(System.getenv("SPARK_HOME"))
    //	   .setJars(List("target/scala-2.10/temporal-graph-project_2.10-1.0.jar","lib/graphx-extensions_2.10-1.0.jar"))

    //For mesos cluster execution use these 2 lines
    val conf = new SparkConf().setMaster("mesos://master:5050").setAppName("TemporalGraph Project")
	.set("spark.executor.uri", "hdfs://master:9000/spark/spark-1.3.1-bin-hadoop2.6.tgz") 

    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)

    var changedType = false
    var startAsMili = System.currentTimeMillis()

      def vAggFunc(a: String, b: String): String = a
      def eAggFunc(a: Int, b: Int): Int = a
      def aggFunc2(a: Double, b: Double): Double = math.max(a, b)

    var result: TemporalGraph[String, Int] = loadData(data, sc, graphType)
    var result2: TemporalGraph[Double, Double] = null

    if (warmStart) {
      //collecting all vertices and edges forces load
      result.vertices.collect
      result.edges.collect
      //reset start time
      println("warm start")
      startAsMili = System.currentTimeMillis()
    }

    for (i <- 0 until args.length) {
      //aggregate operation
      if (args(i) == "--agg") {
        var sem = AggregateSemantics.Existential
        val runWidth: Int = args(i + 1).toInt
        val partAgg: Boolean = if (args(i + 3) == "-p") true else false

        if (args(i + 2) == "universal")
          sem = AggregateSemantics.Universal

        if (partAgg) {
          partitionType = PartitionStrategyType.withName(args(i + 4))
          numParts = args(i + 5).toInt
        }

        var aggStart = System.currentTimeMillis()
        if (changedType) {
          if (partAgg) {
            result2 = result2.partitionBy(partitionType, runWidth, numParts)
          }
          result2 = result2.aggregate(runWidth, sem, aggFunc2, aggFunc2)
        } else {
          if (partAgg) {
            result = result.partitionBy(partitionType, runWidth, numParts)
          }
          result = result.aggregate(runWidth, sem, vAggFunc, eAggFunc)
        }

        var aggEnd = System.currentTimeMillis()
        println("Aggregation Runtime: " + (aggEnd - aggStart) + "ms")
        
      } //select operation 
      else if (args(i) == "--select") {
        val runWidth = 1; //FIXME: is this correct
        val partSel: Boolean = if (args(i + 3) == "-p") true else false

        if (partSel) {
          partitionType = PartitionStrategyType.withName(args(i + 4))
          numParts = args(i + 5).toInt
        }

        var selStart = System.currentTimeMillis()
        if (changedType) {
          if (partSel) {
            result2 = result2.partitionBy(partitionType, runWidth, numParts)
          }
          result2 = result2.select(Interval(args(i + 1).toInt, args(i + 2).toInt))
        } else {
          if (partSel) {
            result = result.partitionBy(partitionType, runWidth, numParts)
          }
          result = result.select(Interval(args(i + 1).toInt, args(i + 2).toInt))
        }
        
        var selEnd = System.currentTimeMillis()
        println("Selection Runtime: " + (selEnd - selStart) + "ms")
        
      } else if (args(i) == "--pagerank") {
        val runWidth = 1; //FIXME: is this correct
        val partPR: Boolean = if (args(i + 2) == "-p") true else false

        if (partPR) {
          partitionType = PartitionStrategyType.withName(args(i + 3))
          numParts = args(i + 4).toInt
        }

        var prStart = System.currentTimeMillis()
        if (changedType) {
          if (partPR) {
            result2 = result2.partitionBy(partitionType, runWidth, numParts)
          }
          result2 = result2.pageRank(true, 0.0001, 0.15, args(i + 1).toInt)
        } else {
          if (partPR) {
            result = result.partitionBy(partitionType, runWidth, numParts)
          }
          result2 = result.pageRank(true, 0.0001, 0.15, args(i + 1).toInt)
          changedType = true
        }
        
        var prEnd = System.currentTimeMillis()
        println("PageRank Runtime: " + (prEnd - prStart) + "ms")
        
      } else if (args(i) == "--count") {
        val runWidth = 1;//FIXME: is this correct
        val partCount: Boolean = if (args(i + 1) == "-p") true else false;
        
        if (partCount) {
          partitionType = PartitionStrategyType.withName(args(i + 2))
          numParts = args(i + 3).toInt
        }

        var ctStart = System.currentTimeMillis()
        if (changedType){
          if(partCount){
            result2 = result2.partitionBy(partitionType, runWidth, numParts)
          }
          println("Total edges across all snapshots: " + result2.numEdges)
        } else {
          if(partCount){
            result = result.partitionBy(partitionType, runWidth, numParts)
          }
          println("Total edges across all snapshots: " + result.numEdges)
        }
        var ctEnd = System.currentTimeMillis()
        println("Count Runtime: " + (ctEnd - ctStart) + "ms")
      }
    }

    val endAsMili = System.currentTimeMillis()
    val runTime = endAsMili - startAsMili
    println("Final Runtime: " + runTime + "ms")

  }

  def loadData(data: String, sc: SparkContext, gtype: String): TemporalGraph[String, Int] = {
    if (gtype == "SG") {
      SnapshotGraph.loadData(data, sc)
    } else if (gtype == "MG") {
      MultiGraph.loadData(data, sc)
    } else if (gtype == "SGP") {
      SnapshotGraphParallel.loadData(data, sc)
    } else
      null
  }
}
