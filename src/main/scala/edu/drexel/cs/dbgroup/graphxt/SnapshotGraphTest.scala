package edu.drexel.cs.dbgroup.graphxt

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.GraphLoader


class SnapshotGraphTestTest {
  final def loadData(dataPath: String, sc:SparkContext): SnapshotGraph[String,Double] = {
    val minYear = 1936
    val maxYear = 2015
    val span = new Interval(minYear, maxYear)
    var years = 0
    val result: SnapshotGraph[String,Double] = new SnapshotGraph(span)

    for (years <- minYear to maxYear) {
      val users = sc.textFile(dataPath + "/nodes/" + years + ".txt").map(line => line.split(",")).map(parts => (parts.head.toLong, parts(1).toString) )
      val edges = GraphLoader.edgeListFile(sc, dataPath + "/edges/" + years + ".txt")
      val graph = edges.outerJoinVertices(users) {
      	case (uid, deg, Some(name)) => name
      	case (uid, deg, None) => ""
      }
      result.addSnapshot(years, partGrapn)
    }
    result
  }

  //TODO: test aggregate on aggregate
  //TODO: test universal semantics
  //TODO: test getSnapshot
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "SnapshotGraph Project", 
      "/Users/vzaychik/spark-1.2.1",
      List("target/scala-2.10/simple-project_2.10-1.0.jar"))

    var testGraph = loadData(args(0), sc)
    val interv = new Interval(1980, 2015)
    val aggregate = testGraph.select(interv).aggregate(5, AggregateSemantics.Existential)
    //there should be 8 results
    println("total number of results after aggregation: " + aggregate.size)
    //let's run pagerank on the aggregate now
    val ranks = aggregate.pageRank(0.0001)
    println("pagerank for each user over time in aggregate:")
    //TODO: do something with ranks like print out top x in each year or whatever
  }

}
