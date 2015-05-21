package edu.drexel.cs.dbgroup.graphxt

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.Graph
import scala.util.control._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object GraphAlgorithmsTest {

  def sgCCtest(datapath: String, sc: SparkContext) {
    var testGraph = SnapshotGraph.loadData(datapath, sc)
    val interv = new Interval(1950, 1952)
    val sel = testGraph.select(interv)

    println("total number of results after selection: " + sel.size)
    println("Selection graphs.size: " + sel.graphs.size)
    //    println("Sel graphs1: " + sel.graphs(0).vertices.collect.mkString("\n"))
    //    println("Sel graphs2: " + sel.graphs(1).vertices.collect.mkString("\n"))
    println("Sel graphs3: " + sel.graphs(2).vertices.collect.mkString("\n"))

    println("\nSelected vertices count: " + sel.graphs.filterNot(_.vertices.isEmpty).map(_.numVertices).reduce(_ + _))
    println("Selected edges count: " + sel.graphs.filterNot(_.edges.isEmpty).map(_.numEdges).reduce(_ + _))

    //run connected components on the selected vertices
    val cc = sel.connectedComponent()
    println("connected components are: ")
    println("CC graphs.size: " + cc.graphs.size)
    //    println("CC graphs1: " + cc.graphs(0).vertices.collect.mkString("\n"));
    //    println("CC graphs2: " + cc.graphs(1).vertices.collect.mkString("\n"));
    println("CC graphs3: " + cc.graphs(2).vertices.collect.mkString("\n"));

    //    println("CC vertices count: " + cc.graphs.filterNot(_.vertices.isEmpty).map(_.numVertices).reduce(_ + _))
    //    println("CC edges count: " + cc.graphs.filterNot(_.edges.isEmpty).map(_.numEdges).reduce(_ + _))

  }

  def sgSPtest(datapath: String, sc: SparkContext) {
    var testGraph = SnapshotGraph.loadData(datapath, sc)
    val interv = new Interval(1954, 1956)
    val sel = testGraph.select(interv)

    println("total number of results after selection: " + sel.size)
    println("Selection graphs.size: " + sel.graphs.size)

    println("\nSelected vertices count: " + sel.graphs.filterNot(_.vertices.isEmpty).map(_.numVertices).reduce(_ + _))
    println("Selected edges count: " + sel.graphs.filterNot(_.edges.isEmpty).map(_.numEdges).reduce(_ + _))

    //run shortest paths on the selected vertices
    var landmarks = Seq(373150L, 68091L, 1383559L, 1218940L, 1218725L)

    val sp = sel.shortestPaths(landmarks)
    println("shortest paths are: ")
    println("SP graphs.size: " + sp.graphs.size)
    //    println("SP graphs1: " + sp.graphs(0).vertices.collect.mkString("\n"));
    //    println("\n")
    println("SP graphs2: " + sp.graphs(1).vertices.collect.mkString("\n"));
    //    println("\n")
    //    println("SP graphs3: " + sp.graphs(2).vertices.collect.mkString("\n"));
  }

  def sgpCCtest(datapath: String, sc: SparkContext) {
    var testGraph = SnapshotGraphParallel.loadData(datapath, sc)
    val interv = new Interval(1950, 1952)
    val sel = testGraph.select(interv)

    println("total number of results after selection: " + sel.size)
    println("Selection graphs.size: " + sel.graphs.size)
    println("Sel graphs1: " + sel.graphs(0).vertices.collect.mkString("\n"))
    println("Sel graphs2: " + sel.graphs(1).vertices.collect.mkString("\n"))
    println("Sel graphs3: " + sel.graphs(2).vertices.collect.mkString("\n"))

    println("\nSelected vertices count: " + sel.graphs.filterNot(_.vertices.isEmpty).map(_.numVertices).reduce(_ + _))
    println("Selected edges count: " + sel.graphs.filterNot(_.edges.isEmpty).map(_.numEdges).reduce(_ + _))

    //run connected components on the selected vertices
    val cc = sel.connectedComponent()
    println("connected components are: ")
    println("CC graphs.size: " + cc.graphs.size)
    println("CC graphs1: " + cc.graphs(0).vertices.collect.mkString("\n"));
    println("CC graphs2: " + cc.graphs(1).vertices.collect.mkString("\n"));
    println("CC graphs3: " + cc.graphs(2).vertices.collect.mkString("\n"));

    //    println("CC vertices count: " + cc.graphs.filterNot(_.vertices.isEmpty).map(_.numVertices).reduce(_ + _))
    //    println("CC edges count: " + cc.graphs.filterNot(_.edges.isEmpty).map(_.numEdges).reduce(_ + _))

  }

  def mgCCtest(datapath: String, sc: SparkContext) {
    var testGraph: MultiGraph[String, Int] = MultiGraph.loadData(datapath, sc)

    //try partitioning
    println("original number of partitions: " + testGraph.edges.partitions.size)
    testGraph = testGraph.partitionBy(PartitionStrategyType.CanonicalRandomVertexCut, 0)

    val sel = testGraph.select(Interval(1954, 1956))
    println("total number of results after selection: " + sel.size)

    val cc = sel.connectedComponent()
    println("connected components are: ")
    println("Selected vertices count: " + cc.graphs.vertices.count)
    println(cc.graphs.vertices.collect.mkString("\n"))

  }

  def main(args: Array[String]) {
    //note: this does not remove ALL logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext("local", "SnapshotGraph Project",
      System.getenv("SPARK_HOME"),
      List("target/scala-2.10/snapshot-graph-project_2.10-1.0.jar"))

    //    sgCCtest(args(0), sc)
    mgCCtest(args(0), sc)
    //    sgpCCtest(args(0), sc)
    //    sgSPtest(args(0), sc)
  }

}
