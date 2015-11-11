package edu.drexel.cs.dbgroup.graphxt.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.Graph
import scala.util.control._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import edu.drexel.cs.dbgroup.graphxt.util.LinearTrendEstimate
import scala.collection.immutable.Map
import java.time.LocalDate

object LinearTrendEstimateTest {

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

    val points = Map(1 -> 2.1, 2 -> 4.5, 3 -> 5.0, 7->3.2, 8->4.3);    
    var result = LinearTrendEstimate.calculateSlope(points)

    println("Resulting slope: " + result)
    println("done")
  }

}
