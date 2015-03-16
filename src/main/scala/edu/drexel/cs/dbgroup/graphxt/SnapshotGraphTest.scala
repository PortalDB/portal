package edu.drexel.cs.dbgroup.graphxt

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.Graph
import scala.util.control._

object SnapshotGraphTest {

  //TODO: test aggregate on aggregate
  //TODO: test universal semantics
  //TODO: test getSnapshot
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "SnapshotGraph Project", 
      System.getenv("SPARK_HOME"),
      List("target/scala-2.10/snapshot-graph-project_2.10-1.0.jar"))

    var testGraph = SnapshotGraph.loadData(args(0), sc)
    val interv = new Interval(1980, 2015)
    val aggregate = testGraph.select(interv).aggregate(5, AggregateSemantics.Existential)
    //there should be 7 results
    println("total number of results after aggregation: " + aggregate.size)
    
    //let's run pagerank on the aggregate now
    val ranks = aggregate.pageRank(0.0001)
    println("pagerank for each user over time in aggregate: ")
   
    //FIXME: what is pagerank supposed to return?
    val iter:Iterator[Interval] = ranks.intervals.keysIterator
    while(iter.hasNext){          
          val k:Interval = iter.next
          println("K: " + k, "--- V: " + ranks.select(k))
    }
    
    //TODO: do something with ranks like print out top x in each year or whatever
  }

}
