package edu.drexel.cs.dbgroup.portal.evaluation

import java.time.LocalDate

import edu.drexel.cs.dbgroup.portal._
import edu.drexel.cs.dbgroup.portal.tools.LocalQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object LocalPoint {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)
    val sqlContext = ProgramContext.getSession

    println("using " + System.getProperty("portal.partitions.sgroup", "") + " sg group")

    sqlContext.emptyDataFrame.count

    val path = args(0)
    val nodesQueriesPath = args(1)
    val edgesQueriesPath = args(2)

    val nodes = Source.fromFile(nodesQueriesPath).getLines.map(l => l.split(',')).map(l => (l(0).toLong, LocalDate.parse(l(1))))
    val edges = Source.fromFile(edgesQueriesPath).getLines.map(l => l.split(',')).map(l => (l(0).toLong, LocalDate.parse(l(1))))

    val lq = new LocalQueries(path)

    val startAsMili = System.currentTimeMillis()

    nodes.foreach { case (id, year) =>
      println("id " + id + " at " + year + ":" + lq.getNode(id, year).collect().mkString(", "))
    }

    println("total time (millis) for " + nodes.size + " local point node queries: " + (System.currentTimeMillis()-startAsMili))

    val startAsMili2 = System.currentTimeMillis()

    edges.foreach { case (id, year) =>
      println("edge " + id + " at " + year + ":" + lq.getEdge(id, year).collect().mkString(", "))
    }

    println("total time (millis) for " + edges.size + " local point edge queries: " + (System.currentTimeMillis()-startAsMili2))

  }

}

