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

  def makeGraph1(): Graph[String, Int] = {
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
    Graph(users, edges, "Default", edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)
  }

  def makeGraph2(): Graph[String, Int] = {
    var users = ProgramContext.sc.parallelize(Array[(VertexId, String)](
      (4L, "Julia"),
      (5L, "Vera"),
      (6L, "Hamlima"),
      (1L, "John")
    ))
    var edges : EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.parallelize(Array(
      Edge(4L, 5L, 12),
      Edge(1L, 4L, 102),
      Edge(1L, 5L, 5)
    )))
    Graph(users, edges, "Default", edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)
  }

  def makeGraph3(): Graph[String, Int] = {
    var users = ProgramContext.sc.parallelize(Array[(VertexId, String)](
      (7L, "Someone"),
      (8L, "Noone"),
      (9L, "Somebody"),
      (4L, "Julia")
    ))
    var edges : EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.parallelize(Array(
      Edge(7L, 8L, 22),
      Edge(8L, 9L, 72),
      Edge(4L, 7L, 5),
      Edge(4L, 8L, 66),
      Edge(4L, 9L, 76)
    )))
    Graph(users, edges, "Default", edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)
  }

  test("select function - the given inverval is present"){
    var intervals: Seq[Interval] = Seq[Interval]()
    var graphs: ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]]()
    //creating the first graph
    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01"))

    intervals = intervals :+ testInterval1 :+ testInterval2 :+ testInterval3
    graphs = graphs :+ makeGraph1() :+ makeGraph2() :+ makeGraph3()

    val sgp = new SnapshotGraphParallel(intervals, graphs)
    val actualSGP = sgp.select(testInterval2)
    val expectedSGP = new SnapshotGraphParallel(Seq[Interval](testInterval2), ParSeq[Graph[String, Int]](makeGraph2()))

    assert(expectedSGP.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSGP.edges.collect() === actualSGP.edges.collect())
  }

  test("select function - the given inverval is present but is non-matching"){
    var intervals: Seq[Interval] = Seq[Interval]()
    var graphs: ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]]()

    //creating the first graph
    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01"))

    intervals = intervals :+ testInterval1 :+ testInterval2 :+ testInterval3
    graphs = graphs :+ makeGraph1() :+ makeGraph2() :+ makeGraph3()

    val sgp = new SnapshotGraphParallel(intervals, graphs)
    val actualSGP = sgp.select(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")))

    val expectedInterval =  Seq[Interval]() :+ testInterval2 :+ testInterval3
    val expectedGraph : ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]]() :+ makeGraph2() :+ makeGraph3()
    val expectedSGP = new SnapshotGraphParallel(expectedInterval, expectedGraph)

    assert(expectedSGP.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSGP.edges.collect() === actualSGP.edges.collect())
  }

  test("select function - the given inverval is outside"){
    var intervals: Seq[Interval] = Seq[Interval]()
    var graphs: ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]]()

    //creating the first graph
    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01"))

    intervals = intervals :+ testInterval1 :+ testInterval2 :+ testInterval3
    graphs = graphs :+ makeGraph1() :+ makeGraph2() :+ makeGraph3()

    val sgp = new SnapshotGraphParallel(intervals, graphs)
    val actualSGP1 = sgp.select(Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")))
    val actualSGP2 = sgp.select(Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")))
    val actualSGP3 = sgp.select(Interval(LocalDate.parse("2019-01-01"), LocalDate.parse("2021-01-01")))

    val expectedSGP =  SnapshotGraphParallel.emptyGraph[String, Int]()

    assert(expectedSGP.vertices.collect() === actualSGP1.vertices.collect())
    assert(expectedSGP.edges.collect() === actualSGP1.edges.collect())
    assert(expectedSGP.edges.collect() === actualSGP2.edges.collect())
    assert(expectedSGP.edges.collect() === actualSGP3.edges.collect())
  }

  test("select function - the given inverval partially overlaps (start is outside the bounds)"){
    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01"))

    val intervals: Seq[Interval] = Seq[Interval](testInterval1, testInterval2, testInterval3)
    val graphs: ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]](makeGraph1(), makeGraph2(), makeGraph3())

    val sgp = new SnapshotGraphParallel(intervals, graphs)
    val actualSGP = sgp.select(Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2016-01-01")))

    val expectedInterval =  Seq[Interval]() :+ testInterval1 :+ testInterval2
    val expectedGraph : ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]]() :+ makeGraph1() :+ makeGraph2()
    val expectedSGP = new SnapshotGraphParallel(expectedInterval, expectedGraph)

    assert(expectedSGP.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSGP.edges.collect() === actualSGP.edges.collect())
  }

  test("select function - the given inverval partially overlaps (end is outside the bounds)"){
    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01"))

    val intervals: Seq[Interval] = Seq[Interval](testInterval1, testInterval2, testInterval3)
    val graphs: ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]](makeGraph1(), makeGraph2(), makeGraph3())

    val sgp = new SnapshotGraphParallel(intervals, graphs)
    val actualSGP = sgp.select(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2019-01-01")))

    val expectedInterval =  Seq[Interval]() :+ testInterval2 :+ testInterval3
    val expectedGraph : ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]](makeGraph2(), makeGraph3())
    val expectedSGP = new SnapshotGraphParallel(expectedInterval, expectedGraph)

    assert(expectedSGP.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSGP.edges.collect() === actualSGP.edges.collect())
  }

  test("select function - the given interval is equal to the graph's interval"){
    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01"))

    val intervals: Seq[Interval] = Seq[Interval](testInterval1, testInterval2, testInterval3)
    val graphs: ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]](makeGraph1(), makeGraph2(), makeGraph3())

    val sgp = new SnapshotGraphParallel(intervals, graphs)
    val actualSGP = sgp.select(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2017-01-01")))
    val expectedSGP = sgp

    assert(expectedSGP.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSGP.edges.collect() === actualSGP.edges.collect())
  }

  test("select function - the graph is empty"){
    val sgp = new SnapshotGraphParallel(Seq[Interval](), ParSeq[Graph[String, Int]]())
    val actualSGP = sgp.select(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2017-01-01")))
    val expectedSGP = sgp

    assert(expectedSGP.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSGP.edges.collect() === actualSGP.edges.collect())
  }
}
