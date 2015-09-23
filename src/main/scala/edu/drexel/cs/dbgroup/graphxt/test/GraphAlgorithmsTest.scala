package edu.drexel.cs.dbgroup.graphxt.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.Graph
import scala.util.control._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import edu.drexel.cs.dbgroup.graphxt._

import java.time.LocalDate

object GraphAlgorithmsTest {

  def sgCCtest(datapath: String) {
    var testGraph = SnapshotGraph.loadData(datapath, LocalDate.parse("1950-01-01"), LocalDate.parse("1953-01-01"))
    val interv = new Interval(LocalDate.parse("1950-01-01"), LocalDate.parse("1953-01-01"))
    val sel = testGraph.select(interv)

    println("total number of results after selection: " + sel.size)
    println("Sel vertices: " + sel.vertices.collect.mkString("\n"))

    println("\nSelected vertices count: " + sel.vertices.count)
    println("Selected edges count: " + sel.edges.count)

    //run connected components on the selected vertices
    val cc = sel.connectedComponents()
    println("connected components are: ")
    println("CC graphs.size: " + cc.size)
    println("CC vertices: " + cc.vertices.collect.mkString("\n"));

  }

  def sgSPtest(datapath: String) {
    var testGraph = SnapshotGraph.loadData(datapath, LocalDate.parse("1954-01-01"), LocalDate.parse("1957-01-01"))
    val interv = new Interval(LocalDate.parse("1954-01-01"), LocalDate.parse("1957-01-01"))
    val sel = testGraph.select(interv)

    println("total number of results after selection: " + sel.size)

    println("\nSelected vertices count: " + sel.vertices.count)
    println("Selected edges count: " + sel.edges.count)

    //run shortest paths on the selected vertices
    var landmarks = Seq(373150L, 68091L, 1383559L, 1218940L, 1218725L)

    val sp = sel.shortestPaths(landmarks)
    println("SG shortest paths are: ")
    println("SP graphs.size: " + sp.size)
    println("SP vertices: " + sp.vertices.collect.mkString("\n"));
  }

  def sgpCCtest(datapath: String) {
    var testGraph = SnapshotGraphParallel.loadData(datapath, LocalDate.parse("1950-01-01"), LocalDate.parse("1953-01-01"))
    val interv = new Interval(LocalDate.parse("1950-01-01"), LocalDate.parse("1953-01-01"))
    val sel = testGraph.select(interv)

    println("total number of results after selection: " + sel.size)
    println("Sel vertices: " + sel.vertices.collect.mkString("\n"))

    println("\nSelected vertices count: " + sel.vertices.count)
    println("Selected edges count: " + sel.edges.count)

    //run connected components on the selected vertices
    val cc = sel.connectedComponents()
    println("connected components are: ")
    println("CC graphs.size: " + cc.size)
    println("CC vertices: " + cc.vertices.collect.mkString("\n"));

  }

  def mgCCtest(datapath: String) {
    var testGraph: TemporalGraph[String, Int] = MultiGraph.loadData(datapath, LocalDate.parse("1954-01-01"), LocalDate.parse("1957-01-01"))

    //try partitioning
    println("original number of partitions: " + testGraph.numPartitions)
    testGraph = testGraph.partitionBy(PartitionStrategyType.CanonicalRandomVertexCut, 0)

    val sel = testGraph.select(Interval(LocalDate.parse("1954-01-01"), LocalDate.parse("1957-01-01")))
    println("total number of results after selection: " + sel.size)

    val cc = sel.connectedComponents()
    println("connected components are: ")
    println("Selected vertices count: " + cc.vertices.count)
    println(cc.vertices.collect.mkString("\n"))

  }

  def mgSPtest(datapath: String) {
    var testGraph: TemporalGraph[String, Int] = MultiGraph.loadData(datapath, LocalDate.parse("1954-01-01"), LocalDate.parse("1957-01-01"))
    var landmarks = Seq(373150L, 1218768L, 1383559L, 1382958L)

    //try partitioning
    testGraph = testGraph.partitionBy(PartitionStrategyType.CanonicalRandomVertexCut, 0)

    val sel = testGraph.select(Interval(LocalDate.parse("1954-01-01"), LocalDate.parse("1957-01-01")))
    println("total number of results after selection: " + sel.size)

    val cc = sel.shortestPaths(landmarks)
    println("Selected vertices count: " + cc.vertices.count)
    println("MG shortest paths are: ")
    println(cc.vertices.collect.mkString("\n"))

  }

  def main(args: Array[String]) {
    //note: this does not remove ALL logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

//    val sc = new SparkContext("local", "SnapshotGraph Project",
//      System.getenv("SPARK_HOME"),
//      List("target/scala-2.10/snapshot-graph-project_2.10-1.0.jar"))

    var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)

    //    sgCCtest(args(0), sc)
    //    mgCCtest(args(0), sc)
    //    sgpCCtest(args(0), sc)
    //    sgSPtest(args(0), sc)
    mgSPtest(args(0))
  }

}
