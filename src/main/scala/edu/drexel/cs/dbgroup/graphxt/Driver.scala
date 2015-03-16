package edu.drexel.cs.dbgroup.graphxt

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.Graph
import scala.util.control._
import scala.collection.mutable.ArrayBuffer

object Driver {
  def main(args: Array[String]) = {
	var graphType: String = ""
    val aggs: ArrayBuffer[Int] = new ArrayBuffer[Int](0)
	val selects: ArrayBuffer[(Int, Int)] = new ArrayBuffer[(Int, Int)](0)
	val pageranks: ArrayBuffer[(Int, Int)] = new ArrayBuffer[(Int, Int)](0)
	var strategy: String = ""
	var iterations: Int = 1
	var data = ""
	
	for(i <- 0 until args.length){
		if(args(i) == "--type"){
			graphType = args(i+1)
		}else if(args(i) == "--strategy"){
			strategy = args(i+1)
		}else if(args(i) == "--iterations"){
			iterations = args(i+1).toInt
		}else if(args(i) == "--data"){
			data = args(i+1)
		}
	}
	
	val sc = new SparkContext("local", "SnapshotGraph Project", 
      System.getenv("SPARK_HOME"),
      List("target/scala-2.10/snapshot-graph-project_2.10-1.0.jar"))
	
	var result = SnapshotGraph.loadData(data, sc)
	val startAsMili = System.currentTimeMillis()
	for(i <- 0 until args.length){
		if(args(i) == "--agg"){
			result = result.aggregate(args(i+1).toInt, AggregateSemantics.Existential)
		}else if(args(i) == "--select"){
			val interv = new Interval(args(i+1).toInt, args(i+2).toInt)
			result = result.select(interv)
		}else if(args(i) == "--pagerank"){
			result = result.pageRank(0.0001)
		}
	}
	val endAsMili = System.currentTimeMillis()
	val runTime = endAsMili.toInt - startAsMili.toInt
	println(runTime)
  }
}
