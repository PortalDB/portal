package edu.drexel.cs.dbgroup.graphxt

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.Graph
import scala.util.control._
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger 
import org.apache.log4j.Level 

object Driver {
  def main(args: Array[String]) = {

  //note: this does not remove ALL logging  
    Logger.getLogger("org").setLevel(Level.OFF) 
    Logger.getLogger("akka").setLevel(Level.OFF) 

    var graphType: String = ""
    var strategy: String = ""
    var iterations: Int = 1
    var data = ""
    var partitionType:PartitionStrategyType.Value = PartitionStrategyType.None

    for(i <- 0 until args.length){
      if(args(i) == "--type"){
	graphType = args(i+1)
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
	
    val sc = new SparkContext("local", "SnapshotGraph Project",
    //val sc = new SparkContext("spark://ec2-54-234-129-137.compute-1.amazonaws.com:7077", "SnapshotGraph Project",
      System.getenv("SPARK_HOME"),
      List("target/scala-2.10/snapshot-graph-project_2.10-1.0.jar"))
    ProgramContext.setContext(sc)

    var result:SnapshotGraph[String,Int] = SnapshotGraph.loadData(data, sc).partitionBy(partitionType)
    println("Number of partitions after partitioning: " + result.numPartitions)
    var result2:SnapshotGraph[Double,Double] = null
    var changedType = false

    val startAsMili = System.currentTimeMillis()

    for(i <- 0 until args.length){
      if(args(i) == "--agg"){
        var sem = AggregateSemantics.Existential
        if (args(i+2) == "universal")
          sem = AggregateSemantics.Universal
        if (changedType) {
	  result2 = result2.aggregate(args(i+1).toInt, sem).partitionBy(partitionType)
          println("Number of partitions after partitioning: " + result2.numPartitions)
        } else {
	  result = result.aggregate(args(i+1).toInt, sem).partitionBy(partitionType)
          println("Number of partitions after partitioning: " + result.numPartitions)
        }
      }else if(args(i) == "--select"){
        if (changedType) {
	  result2 = result2.select(Interval(args(i+1).toInt, args(i+2).toInt)).partitionBy(partitionType)
          println("Number of partitions after partitioning: " + result2.numPartitions)
        } else {
          result = result.select(Interval(args(i+1).toInt, args(i+2).toInt)).partitionBy(partitionType)
          println("Number of partitions after partitioning: " + result.numPartitions)
        }
      }else if(args(i) == "--pagerank"){
        if (changedType) {
	  result2 = result2.pageRank(0.0001,0.15,args(i+1).toInt).partitionBy(partitionType)
          println("Number of partitions after partitioning: " + result2.numPartitions)
        } else {
	  result2 = result.pageRank(0.0001,0.15,args(i+1).toInt).partitionBy(partitionType)
          println("Number of partitions after partitioning: " + result2.numPartitions)
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
}
