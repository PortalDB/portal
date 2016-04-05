package edu.drexel.cs.dbgroup.temporalgraph.representations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import java.time.LocalDate
import edu.drexel.cs.dbgroup.temporalgraph.{Interval, ProgramContext}

import scala.collection.parallel.ParSeq
import org.apache.spark.graphx.{Edge, EdgeRDD, VertexId, Graph}

class SnapshotGraphParallelSuite  extends FunSuite with BeforeAndAfter {

  before {
    if(ProgramContext.sc == null){
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME")).setMaster("local[2]")
      val sc = new SparkContext(conf)
      ProgramContext.setContext(sc)
    }
  }

  test("Testing select method"){
    var intvs: Seq[Interval] = Seq[Interval]()
    var gps: ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]]()

    //creating the first graph
    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01"))
    intvs = intvs :+ testInterval1
    var users: RDD[(VertexId, String)] = ProgramContext.sc.parallelize(Array[(VertexId, String)](
      (1L, "John"),
      (2L, "Mike"),
      (3L, "Ron")
    ))
    var edges: EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.parallelize(Array(
      Edge(1L, 2L, 2),
      Edge(2L, 3L, 32),
      Edge(1L, 3L, 21)
    )))
    val graph1 = Graph(users, edges, "Default", edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)
    gps = gps :+ graph1

    //creating the second graph
    val testInterval2 = Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01"))
    intvs = intvs :+ testInterval2
    users = ProgramContext.sc.parallelize(Array[(VertexId, String)](
      (4L, "Julia"),
      (5L, "Vera"),
      (6L, "Hamlima"),
      (1L, "John")
    ))
    edges = EdgeRDD.fromEdges(ProgramContext.sc.parallelize(Array(
      Edge(4L, 5L, 12),
      Edge(1L, 4L, 102),
      Edge(1L, 5L, 5)
    )))
    val graph2 = Graph(users, edges, "Default", edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)
    gps = gps :+ graph2

    val sgp1 = new SnapshotGraphParallel(intvs, gps)

    val intervalExpected = testInterval2
    val expectedSGP = new SnapshotGraphParallel(Seq[Interval](intervalExpected), ParSeq[Graph[String, Int]](graph2))
    val testSGP = sgp1.select(testInterval2)

    assert(expectedSGP.graphs == testSGP.graphs)
    assert(expectedSGP.intervals == testSGP.intervals)
  }
}
