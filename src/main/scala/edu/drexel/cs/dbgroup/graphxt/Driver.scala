package edu.drexel.cs.dbgroup.graphxt

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph
import scala.util.control._
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.SortedMap
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx.impl.GraphXPartitionExtension._
import java.time.LocalDate

object Driver {
  def main(args: Array[String]) = {

    //note: this does not remove ALL logging  
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    var graphType: String = "SG"
    var strategy: String = ""
    var iterations: Int = 1
    var data = ""
    var partitionType: PartitionStrategyType.Value = PartitionStrategyType.None
    var numParts: Int = -1
    var warmStart = false
    var env = ""

    for (i <- 0 until args.length) {
      if (args(i) == "--type") {
        graphType = args(i + 1)
        if (graphType == "MG")
          println("Running experiments with MultiGraph")
        else if (graphType == "SG")
          println("Running experiments with SnapshotGraph")
        else if (graphType == "SGP")
          println("Running experiments with parallel SnapshotGraph")
        else {
          println("Invalid graph type, exiting")
          System.exit(1)
        }
      } else if (args(i) == "--strategy") {
        strategy = args(i + 1)
      } else if (args(i) == "--iterations") {
        iterations = args(i + 1).toInt
      } else if (args(i) == "--data") {
        data = args(i + 1)
      } else if (args(i) == "--warmstart") {
        warmStart = true
      } 
    }

    // environment specific settings for SparkConf must be passed through the command line
    // settings to pass are master, jars and other configurations
    var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)

    var changedType = false
    var startAsMili = System.currentTimeMillis()

      def vAggFunc(a: String, b: String): String = a
      def eAggFunc(a: Int, b: Int): Int = a
      def aggFunc2(a: Double, b: Double): Double = math.max(a, b)

    var from: LocalDate = LocalDate.MIN
    var to: LocalDate = LocalDate.MAX
    //if there is a select in the query and it comes before others, use it to selectively load
    val loop = new Breaks
    loop.breakable {
      for (i <- 0 until args.length) {
        args(i) match {
          case "--select" => from = LocalDate.parse(args(i + 1)); to = LocalDate.parse(args(i + 2))
          case "--agg" | "--pagerank" | "--count" => loop.break
	  case _ =>
        }
      }
    }
    var result: TemporalGraph[String, Int] = loadData(data, sc, graphType, from, to)
    result.persist()
    var result2: TemporalGraph[Double, Double] = null
    var argNum = 1 //to keep track of the order of arguments passed

    if (warmStart) {
      //collecting all vertices and edges forces load
      result.vertices.collect
      result.edges.collect
      //reset start time
      println("warm start")
      startAsMili = System.currentTimeMillis()
    }

    for (i <- 0 until args.length) {
      //aggregate operation
      if (args(i) == "--agg") {
        var sem = AggregateSemantics.Existential
        var runWidth = 1; 
        val resolution: Resolution = Resolution.from(args(i + 1))
        val partAgg: Boolean = if (args.length > (i + 3) && args(i + 3) == "-p") true else false

        if (args(i + 2) == "universal")
          sem = AggregateSemantics.Universal

        if (partAgg) {
          partitionType = PartitionStrategyType.withName(args(i + 4))
          numParts = args(i + 5).toInt
        }

        var aggStart = System.currentTimeMillis()
        if (changedType) {
          if (partAgg) {
            runWidth = resolution.getNumParts(result2.resolution, LocalDate.now())
            result2 = result2.partitionBy(partitionType, runWidth, numParts)
          }
          result2 = result2.aggregate(resolution, sem, aggFunc2, aggFunc2)
        } else {
          if (partAgg) {
            runWidth = resolution.getNumParts(result.resolution, LocalDate.now())
            result = result.partitionBy(partitionType, runWidth, numParts)
          }
          result = result.aggregate(resolution, sem, vAggFunc, eAggFunc)
        }

        var aggEnd = System.currentTimeMillis()
        var total = aggEnd - aggStart
        println(f"Aggregation Runtime: $total%dms ($argNum%d)")
        argNum += 1

      } //select operation 
      else if (args(i) == "--select") {
        val runWidth = 1; //FIXME: is this correct
        val partSel: Boolean = if (args.length > (i + 3) && args(i + 3) == "-p") true else false

        if (partSel) {
          partitionType = PartitionStrategyType.withName(args(i + 4))
          numParts = args(i + 5).toInt
        }

        var selStart = System.currentTimeMillis()
        if (changedType) {
          if (partSel) {
            result2 = result2.partitionBy(partitionType, runWidth, numParts)
          }
          result2 = result2.select(Interval(LocalDate.parse(args(i + 1)), LocalDate.parse(args(i + 2))))
        } else {
          if (partSel) {
            result = result.partitionBy(partitionType, runWidth, numParts)
          }
          result = result.select(Interval(LocalDate.parse(args(i + 1)), LocalDate.parse(args(i + 2))))
        }

        var selEnd = System.currentTimeMillis()
        var total = selEnd - selStart
        println(f"Selection Runtime: $total%dms ($argNum%d)")
        argNum += 1

      } else if (args(i) == "--pagerank") {
        val runWidth = 1; //FIXME: is this correct
        val partPR: Boolean = if (args.length > (i + 2) && args(i + 2) == "-p") true else false

        if (partPR) {
          partitionType = PartitionStrategyType.withName(args(i + 3))
          numParts = args(i + 4).toInt
        }

        var prStart = System.currentTimeMillis()
        if (changedType) {
          if (partPR) {
            result2 = result2.partitionBy(partitionType, runWidth, numParts)
          }
          result2 = result2.pageRank(true, 0.0001, 0.15, args(i + 1).toInt)
        } else {
          if (partPR) {
            result = result.partitionBy(partitionType, runWidth, numParts)
          }
          result2 = result.pageRank(true, 0.0001, 0.15, args(i + 1).toInt)
          changedType = true
        }

        var prEnd = System.currentTimeMillis()
        var total = prEnd - prStart
        println(f"PageRank Runtime: $total%dms ($argNum%d)")
        argNum += 1

      } else if (args(i) == "--count") {
        val runWidth = 1; //FIXME: is this correct
        val partCount: Boolean = if (args.length > (i + 1) && args(i + 1) == "-p") true else false;

        if (partCount) {
          partitionType = PartitionStrategyType.withName(args(i + 2))
          numParts = args(i + 3).toInt
        }

        var ctStart = System.currentTimeMillis()
        if (changedType) {
          if (partCount) {
            result2 = result2.partitionBy(partitionType, runWidth, numParts)
          }
          println("Total edges across all snapshots: " + result2.edges.count)
        } else {
          if (partCount) {
            result = result.partitionBy(partitionType, runWidth, numParts)
          }
          println("Total edges across all snapshots: " + result.edges.count)
        }
        var ctEnd = System.currentTimeMillis()
        var total = ctEnd - ctStart
        println(f"Count Runtime: $total%dms ($argNum%d)")
        argNum += 1
      }
    }

    val endAsMili = System.currentTimeMillis()
    val runTime = endAsMili - startAsMili
    println(f"Final Runtime: $runTime%dms")
    sc.stop
  }

  def loadData(data: String, sc: SparkContext, gtype: String, from: LocalDate, to: LocalDate): TemporalGraph[String, Int] = {
    if (gtype == "SG") {
      SnapshotGraph.loadData(data, from, to)
    } else if (gtype == "MG") {
      MultiGraph.loadData(data, from, to)
    } else if (gtype == "SGP") {
      SnapshotGraphParallel.loadData(data, from, to)
    } else
      null
  }
}
