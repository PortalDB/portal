package edu.drexel.cs.dbgroup.graphxt.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.Graph
import scala.util.control._
import org.apache.log4j.Logger 
import org.apache.log4j.Level 

import edu.drexel.cs.dbgroup.graphxt._

import java.time.LocalDate

object SnapshotGraphTest {

  def main(args: Array[String]) {
    //note: this does not remove ALL logging
    Logger.getLogger("org").setLevel(Level.OFF) 
    Logger.getLogger("akka").setLevel(Level.OFF) 

    val sc = new SparkContext("local", "SnapshotGraph Project", 
      System.getenv("SPARK_HOME"),
      List("target/scala-2.10/temporal-graph-project_2.10-1.0.jar"))

    def vaggfunc(a: String, b: String): String = { a + b }

    var testGraph = SnapshotGraph.loadData(args(0), LocalDate.parse("1940-01-01"), LocalDate.parse("1949-01-01"))
    val interv = new Interval(LocalDate.parse("1940-01-01"), LocalDate.parse("1949-01-01"))
    val sel = testGraph.select(interv)
    
    println("total number of results after selection: " + sel.size)
    println("Selected vertices count: " + sel.verticesFlat.count)
    println("Selected edges count: " + sel.edgesFlat.count)
    
    val aggregate = sel.aggregate(Resolution.from("PY5"), AggregateSemantics.Existential, vaggfunc, _ + _)

    println("total number of results after aggregation: " + aggregate.size)
    val aggvert = aggregate.verticesFlat
    println("Aggregated vertices count: " + aggvert.count)
    println(aggvert.collect.mkString("\n"))
    val agged = aggregate.edgesFlat
    println("Aggregated edges count: " + agged.count)
    println(agged.collect.mkString("\n"))

    //let's run pagerank on the aggregate now
    val ranks = aggregate.pageRank(true,0.0001, 0.15, 20)
    println("pagerank for each user over time in aggregate: ")
   
  }

}
