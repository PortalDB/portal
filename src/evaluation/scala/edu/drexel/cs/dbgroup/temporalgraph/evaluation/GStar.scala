package edu.drexel.cs.dbgroup.temporalgraph.evaluation

import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.time.LocalDate

import org.apache.spark.graphx.TripletFields
import org.apache.spark.{SparkConf,SparkContext}

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader

object GStar {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)
    val sqlContext = ProgramContext.getSession
    sqlContext.emptyDataFrame.count

    val dataset = args(1)
    val range = Interval(LocalDate.parse(args(2)), LocalDate.parse(args(3)))

    val startAsMili = System.currentTimeMillis()

    args(0).toInt match {
      case 1 => runAverageVertexDegree(dataset, range)
      case 2 => runCCoeffDistro(dataset, range)
      case 3 => runShortestDistDistro(dataset, range, args(4).toLong)
      case 4 => runConCompDistro(dataset, range)
    }

    println("total time (millis) for test case " + args(0).toInt + " in range " + range + ":" + (System.currentTimeMillis()-startAsMili))
    sc.stop()
  }

  //computes average vertex degree for each representative graph in the data range
  def runAverageVertexDegree(data: String, range: Interval): Unit = {
    //load data
    val g = GraphLoader.buildHG(data, -1, -1, range)
    //compute degree per vertex
    val degs = g.aggregateMessages[Int](sendMsg = (et => Iterator((et.dstId,1),(et.srcId,1))), (a,b) => a+b, 0, TripletFields.None)
    //compute one vertex per rg with sum of degrees and count of vertices
    val rgs = degs.vmap((vid, intv, attr) => (attr._2, 1), (0,0)).createAttributeNodes( (a, b) => (a._1 + b._1, a._2 + b._2))((vid,attr) => 1L)
    val result = rgs.vmap((vid, intv, attr) => (attr._1 / attr._2.toDouble), 0.0).vertices
    result.count
  }

  //computes distribution of clustering coefficient for each rep graph in the data range
  def runCCoeffDistro(data: String, range: Interval): Unit = {
    //load data
    val g = GraphLoader.buildHG(data, -1, -1, range)
    //compute clustering coefficient per vertex
    val coeff = g.clusteringCoefficient()
    //compute one vertex per rg per clustering coefficient range
    //i.e., 0-0.1, 0.1-0.2, etc.
    val distro = coeff.vmap((vid, intv, attr) => (math.floor(attr._2*10)/10, 1), (0.0,0)).createAttributeNodes( (a,b) => (a._1, a._2 + b._2))((vid,attr) => (attr._1*10).toLong)
    distro.vertices.count
  }

  //computes distribution of shortest distance to specific vertex per RG
  def runShortestDistDistro(data: String, range: Interval, vid: Long): Unit = {
    //load data
    val g = GraphLoader.buildHG(data, -1, -1, range)
    //compute shortest path
    val sp = g.shortestPaths(true, Seq(vid))
    //compute one vertex per rg per distance
    val distro = sp.vmap((vid, intv, attr) => (attr._2.getOrDefault(vid, Int.MaxValue), 1), (Int.MaxValue,0)).createAttributeNodes( (a,b) => (a._1, a._2 + b._2))((vid,attr) => attr._1.toLong)
    distro.vertices.count
  }

  //computes distribution of connected component sizes per RG
  def runConCompDistro(data: String, range: Interval): Unit = {
    //load data
    val g = GraphLoader.buildHG(data, -1, -1, range)
    //compute connected components
    val ccs = g.connectedComponents()
    //compute one vertex per component
    val comps = ccs.vmap((vid, intv, attr) => (attr._2, 1), (0L,0)).createAttributeNodes( (a,b) => (a._1, a._2+b._2))((vid,attr) => attr._1)
    //compute one vertex per component size for distribution
    val distro = comps.vmap((vid, intv, attr) => (attr._2, 0), (0,0)).createAttributeNodes( (a,b) => (a._1, a._2+b._2))((vid,attr) => attr._1)
    distro.vertices.count
  }

}
