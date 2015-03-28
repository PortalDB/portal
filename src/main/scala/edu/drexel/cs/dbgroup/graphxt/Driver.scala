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

    for(i <- 0 until args.length){
      if(args(i) == "--type"){
	graphType = args(i+1)
        if (graphType == "MG")
          println("Running experiments with MultiGraph")
        else if (graphType == "SG")
          println("Running experiments with SnapshotGraph")
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
      }
    }
	
    val sc = new SparkContext("local", "TemporalGraph Project",
    //val sc = new SparkContext("spark://ec2-54-234-129-137.compute-1.amazonaws.com:7077", "TemporalGraph Project",
      System.getenv("SPARK_HOME"),
      List("target/scala-2.10/temporal-graph-project_2.10-1.0.jar","lib/graphx-extensions_2.10-1.0.jar"))
    ProgramContext.setContext(sc)

    var changedType = false
    val startAsMili = System.currentTimeMillis()

    //for snapshotgraph tests
    if (graphType == "SG") {
      var result:SnapshotGraph[String,Int] = SnapshotGraph.loadData(data, sc)
      var result2:SnapshotGraph[Double,Double] = null

      for(i <- 0 until args.length){
        if(args(i) == "--agg"){
          var sem = AggregateSemantics.Existential
          if (args(i+2) == "universal")
            sem = AggregateSemantics.Universal
          val runWidth:Int = args(i+1).toInt
          if (changedType) {
	    result2 = result2.partitionBy(partitionType,runWidth).aggregate(runWidth, sem)
          } else {
	    result = result.partitionBy(partitionType,runWidth).aggregate(runWidth, sem)
          }
        }else if(args(i) == "--select"){
          if (changedType) {
	    result2 = result2.select(Interval(args(i+1).toInt, args(i+2).toInt))
          } else {
            result = result.select(Interval(args(i+1).toInt, args(i+2).toInt))
          }
        }else if(args(i) == "--pagerank"){
          if (changedType) {
	    result2 = result2.pageRank(0.0001,0.15,args(i+1).toInt)
          } else {
	    result2 = result.pageRank(0.0001,0.15,args(i+1).toInt)
            changedType = true
          }
        }else if(args(i) == "--count"){
          if (changedType)
            println("Total edges across all snapshots: " + result2.partitionBy(PartitionStrategyType.NaiveTemporal,0).numEdges)
          else
            println("Total edges across all snapshots: " + result.partitionBy(PartitionStrategyType.NaiveTemporal,0).numEdges)
        }
      }
    } else { //multigraph
      var result:MultiGraph[String,Int] = MultiGraph.loadGraph(data, sc)
      var result2:MultiGraph[Seq[Double],Double] = null

      for(i <- 0 until args.length){
        if(args(i) == "--agg"){
          var sem = AggregateSemantics.Existential
          if (args(i+2) == "universal")
            sem = AggregateSemantics.Universal
          val runWidth:Int = args(i+1).toInt
          if (changedType) {
	    result2 = result2.partitionBy(partitionType,runWidth).aggregate(runWidth, sem)
          } else {
	    result = result.partitionBy(partitionType,runWidth).aggregate(runWidth, sem)
          }
        }else if(args(i) == "--select"){
          if (changedType) {
	    result2 = result2.partitionBy(partitionType,1).select(Interval(args(i+1).toInt, args(i+2).toInt))
          } else {
            result = result.partitionBy(partitionType,1).select(Interval(args(i+1).toInt, args(i+2).toInt))
          }
        }else if(args(i) == "--pagerank"){
          if (changedType) {
	    result2 = result2.partitionBy(partitionType,1).pageRank(0.0001,0.15,args(i+1).toInt)
          } else {
	    result2 = result.partitionBy(partitionType,1).pageRank(0.0001,0.15,args(i+1).toInt)
            changedType = true
          }
        }else if(args(i) == "--count"){
          if (changedType)
            println("Total edges across all snapshots: " + result2.partitionBy(partitionType,1).graphs.edges.count)
          else
            println("Total edges across all snapshots: " + result.partitionBy(partitionType,1).graphs.edges.count)
        }
      }
    }

    val endAsMili = System.currentTimeMillis()
    val runTime = endAsMili - startAsMili
    println("Final Runtime: " + runTime + "ms")

  }
}
