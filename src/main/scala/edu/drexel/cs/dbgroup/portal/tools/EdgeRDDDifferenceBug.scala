package edu.drexel.cs.dbgroup.portal.tools

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}


object EdgeRDDDifferenceBug {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME")).setMaster("local[2]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit ={
    val testNodes: RDD[(VertexId, String)] = sc.parallelize(Array(
      (1L, "John"),
      (2L, "Mike"),
      (3L, "Ron"),
      (4L, "John")
    ))
    val testEdges: RDD[Edge[Int]] = sc.parallelize(Array(
      Edge(1L, 2L, 42),
      Edge(2L, 3L, 42),
      Edge(1L, 3L, 42)
    ))
    val graph1 = Graph(testNodes, testEdges, "Default")

    val testNodes2: RDD[(VertexId, String)] = sc.parallelize(Array(
      (1L, "John"),
      (2L, "Mike"),
      (3L, "Ron")
    ))
    val testEdges2: RDD[Edge[Int]] = sc.parallelize(Array(
      Edge(1L, 3L, 42)
    ))
    val graph2 = Graph(testNodes2, testEdges2, "Default")

    val difference = graph1.edges.subtract(graph2.edges)
    difference.foreach(println)
  }

}
