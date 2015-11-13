package edu.drexel.cs.dbgroup.graphxt.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import scala.util.control._
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.SortedMap
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx.impl.GraphXPartitionExtension._
import java.time.LocalDate

import edu.drexel.cs.dbgroup.graphxt._
import edu.drexel.cs.dbgroup.graphxt.util.LinearTrendEstimate

object ComplexQueryTest {
  def main(args: Array[String]) = {

    //note: this does not remove ALL logging  
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    var graphType: String = "SG"
    var data = ""
    var partitionType: PartitionStrategyType.Value = PartitionStrategyType.None
    var from: LocalDate = LocalDate.MIN
    var to: LocalDate = LocalDate.MAX

    for (i <- 0 until args.length) {
      if (args(i) == "--type") {
        graphType = args(i + 1)
        graphType match {
          case "MG" =>
            println("Running experiments with MultiGraph")
          case "SG" =>
            println("Running experiments with SnapshotGraph")
          case "SGP" =>
            println("Running experiments with parallel SnapshotGraph")
          case "MGC" =>
            println("Running experiments with columnar MultiGraph")
          case "OG" =>
            println("Running experiments with OneGraph")
          case "OGC" =>
            println("Running experiments with columnar OneGraph")
          case _ =>
            println("Invalid graph type, exiting")
            System.exit(1)
        }
      } else if (args(i) == "--data") {
        data = args(i + 1)
      } else if (args(i) == "--from") {
        from = LocalDate.parse(args(i + 1))
      } else if (args(i) == "--to") {
        to = LocalDate.parse(args(i + 1))
      } else if (args(i) == "--strategy") {
        partitionType = PartitionStrategyType.withName(args(i + 1))
      }
    }

    // environment specific settings for SparkConf must be passed through the command line
    // settings to pass are master, jars and other configurations
    var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)

    var startAsMili = System.currentTimeMillis()

    val selStart = System.currentTimeMillis()
    val result: TemporalGraph[String, Int] = loadData(data, sc, graphType, from, to, partitionType, 1).persist()
    result.materialize
    var total = System.currentTimeMillis() - selStart
    println(f"Select Runtime: $total%dms (1)")

    val aggStart = System.currentTimeMillis()
    //now aggregate
    def vAggFunc(a: String, b: String): String = a
    def eAggFunc(a: Int, b: Int): Int = a + b
    val result2: TemporalGraph[String, Int] = result.aggregate(Resolution.from("P8Y"), AggregateSemantics.Universal, vAggFunc, eAggFunc).persist()
    result.unpersist()
    result2.materialize
    total = System.currentTimeMillis() - aggStart
    println(f"Aggregate Runtime: $total%dms (2)")

    //now pagerank
    val prStart = System.currentTimeMillis()
    val result3: TemporalGraph[Double,Double] = result2.pageRank(true, 0.001, 0.15, 10).persist()
    result2.unpersist()
    result3.materialize
    total = System.currentTimeMillis() - prStart
    println(f"Pagerank Runtime: $total%dms (3)")

    //now aggregate for the total
    val trendStart = System.currentTimeMillis()
    val intervals = result3.getTemporalSequence
    def vmap(vid: VertexId, in: Interval, vv: Double): Map[Int,Double] = {
      Map(intervals.indexOf(in) -> vv)
    }
    //this should work because no keys should be overlapping
    def listFunc(a: Map[Int,Double], b: Map[Int,Double]) = a ++ b
    def sumFunc(a: Double, b: Double) = a + b
    def vmap2(vid: VertexId, in: Interval, vv: Map[Int,Double]): Double = {
      LinearTrendEstimate.calculateSlope(vv)
    }
    val result4: TemporalGraph[Double,Double] = result3
      .mapVertices(vmap)
      .aggregate(Resolution.between(from, to), AggregateSemantics.Existential, listFunc, sumFunc)
      .mapVertices(vmap2)
      .persist()
    result3.unpersist()
    result4.materialize
    total = System.currentTimeMillis() - trendStart
    println(f"Trend Runtime: $total%dms (4)")
    
    val endAsMili = System.currentTimeMillis()
    val runTime = endAsMili - startAsMili
    println(f"Final Runtime: $runTime%dms")

    //results just to see
    println("Top 10 by trend:")
    println(result4.verticesFlat.sortBy(c => c._2._2, false).take(10).mkString("\n"))

    sc.stop
  }

  def loadData(data: String, sc: SparkContext, gtype: String, from: LocalDate, to: LocalDate, strategy: PartitionStrategyType.Value, runWidth: Int): TemporalGraph[String, Int] = {
    println("Loading data with " + gtype + " data structure, using " + strategy.toString + " strategy and " + runWidth + " runWidth")
    gtype match {
      case "SG" =>
        SnapshotGraph.loadData(data, from, to)
      case "MG" =>
        MultiGraph.loadWithPartition(data, from, to, strategy, runWidth)
      case "SGP" =>
        SnapshotGraphParallel.loadWithPartition(data, from, to, strategy, runWidth)
      case "MGC" =>
        MultiGraphColumn.loadWithPartition(data, from, to, strategy, runWidth)
      case "OG" =>
        OneGraph.loadWithPartition(data, from, to, strategy, runWidth)
      case "OGC" =>
        OneGraphColumn.loadWithPartition(data, from, to, strategy, runWidth)
      case _ =>
        null
    }
  }
}
