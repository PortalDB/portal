package edu.drexel.cs.dbgroup.graphxt

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger 
import org.apache.log4j.Level 

object MultiGraphTest {

  def main(args: Array[String]) {

    //note: this does not remove ALL logging
    Logger.getLogger("org").setLevel(Level.OFF) 
    Logger.getLogger("akka").setLevel(Level.OFF) 

    val sc = new SparkContext("local", "MultiGraph Project", 
      System.getenv("SPARK_HOME"),
      List("target/scala-2.10/multigraph-project_2.10-1.0.jar"))

    var testGraph = MultiGraph.loadGraph(args(0), sc)

    //try partitioning
    testGraph = testGraph.partitionBy(PartitionStrategyType.CanonicalRandomVertexCut,0)

    val sel = testGraph.select(Interval(1940,1948))
    println("total number of results after selection: " + sel.size)
    println("Selected vertices count: " + sel.graphs.vertices.count)
    println(sel.graphs.vertices.collect.mkString("\n"))
    println("Selected edges count: " + sel.graphs.edges.count)
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
    val ranks = aggregate.pageRank(0.0001, 0.15, 50)
    println("done")
  }

}
