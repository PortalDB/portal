package edu.drexel.cs.dbgroup.temporalgraph.evaluation

import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.time.LocalDate

import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.TripletFields
import org.apache.spark.{SparkConf,SparkContext}

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader

/**
  * Run experiments on Portal to compare against GStar
  * 1. Average Vertex Degree
  * 2. Distribution of Clustering Coefficient
  * 3. Distribution of shortest distance
  * 4. Distribution of the size of connected components
  * 5. Case 1 with no coalescing
  * 6. Case 2 with no coalescing
  * 7. Case 3 with no coalescing
  * 8. Case 4 with no coalescing
  * 9. Case 1 with SQL for final aggregations
  * 10.Case 2 with SQL for final aggregations
  * 11.Case 3 with SQL for final aggregations
  * 12.Case 4 with SQL for final aggregations
  * 
  * Run-time arguments:
  * 1. which of the above cases to run
  * 2. on which dataset (uri)
  * 3. start date for the range of analysis
  * 4. end date for the range of analysis
  * 5. id of the vertex for the shortest path case
  * 
*/ 

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
      case 1 => runAverageVertexDegree(dataset, range, true)
      case 2 => runCCoeffDistro(dataset, range, true)
      case 3 => runShortestDistDistro(dataset, range, args(4).toLong, true)
      case 4 => runConCompDistro(dataset, range, true)
      case 5 => runAverageVertexDegree(dataset, range, false)
      case 6 => runCCoeffDistro(dataset, range, false)
      case 7 => runShortestDistDistro(dataset, range, args(4).toLong, false)
      case 8 => runConCompDistro(dataset, range, false)
      case 9 => runAverageVertexDegreeSQL(dataset, range)
      case 10 => runCCoeffDistroSQL(dataset, range)
      case 11 => runShortestDistDistroSQL(dataset, range, args(4).toLong)
      case 12 => runConCompDistroSQL(dataset, range)
    }

    println("total time (millis) for test case " + args(0).toInt + " in range " + range + ":" + (System.currentTimeMillis()-startAsMili))
    sc.stop()
  }

  //computes average vertex degree for each representative graph in the data range
  def runAverageVertexDegree(data: String, range: Interval, coal: Boolean): Unit = {
    //load data
    val g = GraphLoader.buildVE(data, -1, -1, range)
    //compute degree per vertex
    val degs = g.aggregateMessages[Int](sendMsg = (et => Iterator((et.dstId,1),(et.srcId,1))), (a,b) => a+b, 0, TripletFields.None)
    //compute one vertex per rg with sum of degrees and count of vertices
    val rgs = degs.vmap((vid, intv, attr) => (attr._2, 1), (0,0)).createAttributeNodes( (a, b) => (a._1 + b._1, a._2 + b._2))((vid,attr) => 1L)
    val result = rgs.vmap((vid, intv, attr) => (attr._1 / attr._2.toDouble), 0.0)
    if (coal)
      result.vertices.count
    else
      result.allVertices.count
  }

  //computes distribution of clustering coefficient for each rep graph in the data range
  def runCCoeffDistro(data: String, range: Interval, coal: Boolean): Unit = {
    //load data
    val g = GraphLoader.buildHG(data, -1, -1, range)
    //compute clustering coefficient per vertex
    val coeff = g.clusteringCoefficient()
    //compute one vertex per rg per clustering coefficient range
    //i.e., 0-0.1, 0.1-0.2, etc.
    val distro = coeff.vmap((vid, intv, attr) => (math.floor(attr._2*10)/10, 1), (0.0,0)).createAttributeNodes( (a,b) => (a._1, a._2 + b._2))((vid,attr) => (attr._1*10).toLong)
    if (coal)
      distro.vertices.count
    else
      distro.allVertices.count
  }

  //computes distribution of shortest distance to specific vertex per RG
  def runShortestDistDistro(data: String, range: Interval, src: Long, coal: Boolean): Unit = {
    //load data
    val g = GraphLoader.buildHG(data, -1, -1, range)
    //compute shortest path
    val sp = g.shortestPaths(true, Seq(src))
    //compute one vertex per rg per distance
    val distro = sp.vmap((vid, intv, attr) => (attr._2.getOrDefault(src, Int.MaxValue), 1), (Int.MaxValue,0)).createAttributeNodes( (a,b) => (a._1, a._2 + b._2))((vid,attr) => attr._1.toLong)
    if (coal)
      distro.vertices.count
    else
      distro.allVertices.count
  }

  //computes distribution of connected component sizes per RG
  def runConCompDistro(data: String, range: Interval, coal: Boolean): Unit = {
    //load data
    val g = GraphLoader.buildHG(data, -1, -1, range)
    //compute connected components
    val ccs = g.connectedComponents()
    //compute one vertex per component
    val comps = ccs.vmap((vid, intv, attr) => (attr._2, 1), (0L,0)).createAttributeNodes( (a,b) => (a._1, a._2+b._2))((vid,attr) => attr._1)
    //compute one vertex per component size for distribution
    val distro = comps.vmap((vid, intv, attr) => (attr._2, 0), (0,0)).createAttributeNodes( (a,b) => (a._1, a._2+b._2))((vid,attr) => attr._1)
    if (coal)
      distro.vertices.count
    else
      distro.allVertices.count
  }

  //computes average vertex degree for each representative graph in the data range
  //but uses sparksql for aggregation
  def runAverageVertexDegreeSQL(data: String, range: Interval): Unit = {
    //load data
    val g = GraphLoader.buildVE(data, -1, -1, range)
    //compute degree per vertex
    val degs = g.aggregateMessages[Int](sendMsg = (et => Iterator((et.dstId,1),(et.srcId,1))), (a,b) => a+b, 0, TripletFields.None)
    val df = makeDataFrameInt(degs.vmap((vid, intv, attr) => attr._2, 0).allVertices)
    df.groupBy("estart").agg(avg("attr")).count
  }

  //computes distribution of clustering coefficient for each rep graph in the data range
  def runCCoeffDistroSQL(data: String, range: Interval): Unit = {
    //load data
    val g = GraphLoader.buildHG(data, -1, -1, range)
    //compute clustering coefficient per vertex
    val coeff = g.clusteringCoefficient()
    val df = makeDataFrameDouble(coeff.vmap((vid, intv, attr) => math.floor(attr._2*10)/10, 0.0).allVertices)
    df.groupBy("estart", "attr").agg(count("vid")).count
  }

  //computes distribution of shortest distance to specific vertex per RG
  def runShortestDistDistroSQL(data: String, range: Interval, src: Long): Unit = {
    //load data
    val g = GraphLoader.buildHG(data, -1, -1, range)
    //compute shortest path
    val sp = g.shortestPaths(true, Seq(src))
    val df = makeDataFrameInt(sp.vmap((vid, intv, attr) => attr._2.getOrDefault(src, Int.MaxValue), Int.MaxValue).allVertices)
    df.groupBy("estart", "attr").agg(count("vid")).count
  }

  //computes distribution of connected component sizes per RG
  def runConCompDistroSQL(data: String, range: Interval): Unit = {
    //load data
    val g = GraphLoader.buildHG(data, -1, -1, range)
    //compute connected components
    val ccs = g.connectedComponents()
    val df = makeDataFrameLong(ccs.vmap((vid, intv, attr) => attr._2, -1L).allVertices)
    df.groupBy("estart", "attr").agg(count("vid").as("comp_size")).groupBy("estart","comp_size").agg(count("attr")).count
  }

  def makeDataFrameDouble(rdd: RDD[(Long, (Interval,Double))]): DataFrame = {
    val rowRdd = rdd.map(x => Row(x._1, java.sql.Date.valueOf(x._2._1.start), java.sql.Date.valueOf(x._2._1.end), x._2._2))
    val schema = StructType(StructField("vid", LongType, false) ::
      StructField("estart", DateType, false) ::
      StructField("eend", DateType, false) ::
      StructField("attr", DoubleType, false) :: Nil)
    ProgramContext.getSession.createDataFrame(rowRdd, schema)
  }

  def makeDataFrameLong(rdd: RDD[(Long, (Interval,Long))]): DataFrame = {
    val rowRdd = rdd.map(x => Row(x._1, java.sql.Date.valueOf(x._2._1.start), java.sql.Date.valueOf(x._2._1.end), x._2._2))
    val schema = StructType(StructField("vid", LongType, false) ::
      StructField("estart", DateType, false) ::
      StructField("eend", DateType, false) ::
      StructField("attr", LongType, false) :: Nil)
    ProgramContext.getSession.createDataFrame(rowRdd, schema)
  }

  def makeDataFrameInt(rdd: RDD[(Long, (Interval,Int))]): DataFrame = {
    val rowRdd = rdd.map(x => Row(x._1, java.sql.Date.valueOf(x._2._1.start), java.sql.Date.valueOf(x._2._1.end), x._2._2))
    val schema = StructType(StructField("vid", LongType, false) ::
      StructField("estart", DateType, false) ::
      StructField("eend", DateType, false) ::
      StructField("attr", IntegerType, false) :: Nil)
    ProgramContext.getSession.createDataFrame(rowRdd, schema)
  }

}
