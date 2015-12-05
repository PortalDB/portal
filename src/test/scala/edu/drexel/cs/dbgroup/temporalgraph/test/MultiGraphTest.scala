package edu.drexel.cs.dbgroup.graphxt.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger 
import org.apache.log4j.Level 

import edu.drexel.cs.dbgroup.graphxt._

import java.time.LocalDate

object MultiGraphTest {

  def main(args: Array[String]) {

    //note: this does not remove ALL logging
    Logger.getLogger("org").setLevel(Level.OFF) 
    Logger.getLogger("akka").setLevel(Level.OFF) 

    var conf = new SparkConf()
      conf = new SparkConf().setMaster("local").setAppName("TemporalGraph Project")
        .setSparkHome(System.getenv("SPARK_HOME"))
        .setJars(List("target/scala-2.10/temporal-graph-project_2.10-1.0.jar", "lib/graphx-extensions_2.10-1.0.jar"))
        .set("spark.executor.memory", "6g")

    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)

    var testGraph:TemporalGraph[String,Int] = MultiGraph.loadData(args(0), LocalDate.parse("1940-01-01"), LocalDate.parse("1949-01-01"))

    //try partitioning
    println("original number of partitions: " + testGraph.edges.partitions.size)
    testGraph = testGraph.partitionBy(PartitionStrategyType.CanonicalRandomVertexCut,0)

    val sel = testGraph.select(Interval(LocalDate.parse("1940-01-01"),LocalDate.parse("1940-01-01")))
    println("total number of results after selection: " + sel.size) //should be 10
    println("Selected vertices count: " + sel.vertices.count) //should be 75
    println(sel.vertices.collect.mkString("\n"))
    println("Selected edges count: " + sel.edges.count) //should be 13
    println(sel.edges.collect.mkString("\n"))

    val aggregate = sel.aggregate(Resolution.from("PY5"), AggregateSemantics.Existential, _ + _, _ + _)
    //there should be 2 results
    println("total number of results after aggregation: " + aggregate.size)
    println("Aggregated vertices count: " + aggregate.vertices.count)
    println(aggregate.vertices.collect.mkString("\n"))
    println("Aggregated edges count: " + aggregate.edges.count)
    println(aggregate.edges.collect.mkString("\n"))
    
    //let's run pagerank on the aggregate now
    val ranks = aggregate.pageRank(true,0.0001, 0.15, 50)
    println("done")
  }

}
