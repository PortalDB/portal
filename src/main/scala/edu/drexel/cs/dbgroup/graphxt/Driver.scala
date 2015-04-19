package edu.drexel.cs.dbgroup.graphxt

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
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
    var partitionType:PartitionStrategyType.Value = PartitionStrategyType.None
    var numParts: Int = -1
    var warmStart = false

    for(i <- 0 until args.length){
      if(args(i) == "--type"){
	graphType = args(i+1)
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
      }else if(args(i) == "--strategy"){
	strategy = args(i+1)
      }else if(args(i) == "--iterations"){
	iterations = args(i+1).toInt
      }else if(args(i) == "--data"){
	data = args(i+1)
      }else if (args(i) == "--partition"){
        partitionType = PartitionStrategyType.withName(args(i+1))

        if(args.length > i+2){
          numParts = args(i+2).toInt
        }
      }else if (args(i) == "--warmstart"){
        warmStart = true
      }
    }
	
    val sc = new SparkContext("local", "TemporalGraph Project",
    //val sc = new SparkContext("spark://ec2-54-234-129-137.compute-1.amazonaws.com:7077", "TemporalGraph Project",
      System.getenv("SPARK_HOME"),
      List("target/scala-2.10/temporal-graph-project_2.10-1.0.jar","lib/graphx-extensions_2.10-1.0.jar"))
    ProgramContext.setContext(sc)

    var changedType = false
    var startAsMili = System.currentTimeMillis()

    def vAggFunc(a: String, b: String): String = a
    def eAggFunc(a: Int, b: Int):Int = a
    def aggFunc2(a: Double, b: Double):Double = math.max(a,b)

    var result:TemporalGraph[String,Int] = loadData(data, sc, graphType)
    var result2:TemporalGraph[Double,Double] = null

    if (warmStart) {
      //collecting all vertices and edges forces load
      result.vertices.collect
      result.edges.collect
      //reset start time
      println("warm start")
      startAsMili = System.currentTimeMillis()
    }

    for(i <- 0 until args.length){
      if(args(i) == "--agg"){
        var sem = AggregateSemantics.Existential
        if (args(i+2) == "universal")
          sem = AggregateSemantics.Universal
        val runWidth:Int = args(i+1).toInt
        if (changedType) {
	  result2 = result2.partitionBy(partitionType, runWidth, numParts).aggregate(runWidth, sem, aggFunc2, aggFunc2)
        } else {
	  result = result.partitionBy(partitionType, runWidth, numParts).aggregate(runWidth, sem, vAggFunc, eAggFunc)
        }
      }else if(args(i) == "--select"){
        if (changedType) {
	  result2 = result2.select(Interval(args(i+1).toInt, args(i+2).toInt))
        } else {
          result = result.select(Interval(args(i+1).toInt, args(i+2).toInt))
        }
      }else if(args(i) == "--pagerank"){
        if (changedType) {
	  result2 = result2.pageRank(true,0.0001,0.15,args(i+1).toInt)
        } else {
	  result2 = result.pageRank(true,0.0001,0.15,args(i+1).toInt)
          changedType = true
        }
      }else if(args(i) == "--count"){
        if (changedType)
          println("Total edges across all snapshots: " + result2.numEdges)
        else
          println("Total edges across all snapshots: " + result.numEdges)
      }
    }

    val endAsMili = System.currentTimeMillis()
    val runTime = endAsMili - startAsMili
    println("Final Runtime: " + runTime + "ms")

  }

  def loadData(data: String, sc: SparkContext, gtype: String):TemporalGraph[String,Int] = {
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
