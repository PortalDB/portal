package edu.drexel.cs.dbgroup.graphxt.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger 
import org.apache.log4j.Level 

import edu.drexel.cs.dbgroup.graphxt._

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

    var testGraph:MultiGraph[String,Int] = MultiGraph.loadData(args(0), 1940, 1948)

    //try partitioning
    println("original number of partitions: " + testGraph.edges.partitions.size)
    testGraph = testGraph.partitionBy(PartitionStrategyType.CanonicalRandomVertexCut,0)

    val sel = testGraph.select(Interval(1940,1948))
    println("total number of results after selection: " + sel.size) //should be 10
    println("Selected vertices count: " + sel.graphs.vertices.count) //should be 75
    println(sel.graphs.vertices.collect.mkString("\n"))
    println("Selected edges count: " + sel.graphs.edges.count) //should be 13
    println(sel.graphs.edges.collect.mkString("\n"))

    val aggregate = sel.aggregate(5, AggregateSemantics.Existential, _ + _, _ + _)
    //there should be 2 results
    println("total number of results after aggregation: " + aggregate.size)
    val iter:Iterator[Interval] = aggregate.intervals.keysIterator
    while (iter.hasNext) {
      val nx:Interval = iter.next
      println(nx.min + "-" + nx.max)
    }
    println("Aggregated vertices count: " + aggregate.graphs.vertices.count)
    println(aggregate.graphs.vertices.collect.mkString("\n"))
    println("Aggregated edges count: " + aggregate.graphs.edges.count)
    println(aggregate.graphs.edges.collect.mkString("\n"))
    
    //let's run pagerank on the aggregate now
    val ranks = aggregate.pageRank(true,0.0001, 0.15, 50)
    println("done")
  }

}
