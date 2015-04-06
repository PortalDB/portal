package edu.drexel.cs.dbgroup.graphxt

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.Graph
import scala.util.control._
import org.apache.log4j.Logger 
import org.apache.log4j.Level 

object SnapshotGraphTest {

  def main(args: Array[String]) {
    //note: this does not remove ALL logging
    Logger.getLogger("org").setLevel(Level.OFF) 
    Logger.getLogger("akka").setLevel(Level.OFF) 

    val sc = new SparkContext("local", "SnapshotGraph Project", 
      System.getenv("SPARK_HOME"),
      List("target/scala-2.10/snapshot-graph-project_2.10-1.0.jar"))

    def vaggfunc(a: String, b: String): String = { a + b }

    var testGraph = SnapshotGraph.loadData(args(0), sc)
    val interv = new Interval(1940, 1948)
    val aggregate = testGraph.select(interv).aggregate(5, AggregateSemantics.Existential, vaggfunc, _ + _)

    println("total number of results after aggregation: " + aggregate.size)
    println("Aggregated vertices count 1: " + aggregate.graphs(0).numVertices)
    println("Aggregated vertices count 2: " + aggregate.graphs(1).numVertices)
    println(aggregate.graphs(0).vertices.collect.mkString("\n"))
    println("second set")
    println(aggregate.graphs(1).vertices.collect.mkString("\n"))
    println("Aggregated edges count: " + aggregate.numEdges)
    println(aggregate.graphs(0).edges.collect.mkString("\n"))
    println("second set")
    println(aggregate.graphs(1).edges.collect.mkString("\n"))

    //let's run pagerank on the aggregate now
    val ranks = aggregate.pageRank(0.0001, 0.15, 20)
    println("pagerank for each user over time in aggregate: ")
   
    val iter:Iterator[Interval] = ranks.intervals.keysIterator
    while(iter.hasNext){          
      val k:Interval = iter.next
      println("K: " + k, "--- V: ")
      println(ranks.getSnapshotByPosition(ranks.intervals(k)).vertices.collect.mkString("\n"))
    }
    
  }

}
