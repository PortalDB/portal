package edu.drexel.cs.dbgroup.temporalgraph.representations

import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.parallel.ParSeq

/**
  * Created by shishir on 5/10/2016.
  */
class OneGraphColumnSuite extends FunSuite with BeforeAndAfter{
  before {
    if(ProgramContext.sc == null){
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      val conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME")).setMaster("local[2]")
      val sc = new SparkContext(conf)
      ProgramContext.setContext(sc)
      println(" ") //the first line starts from between
    }
  }

  test("slice function"){
    //Regular cases
    val sliceInterval = (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")))
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 22)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 22)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22))
    ))
    val ogc = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke"))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((3L, 5L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), 22)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22))
    ))
    val expectedOGC = OneGraphColumn.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER )

    var actualOGC = ogc.slice(sliceInterval)

    assert(expectedOGC.vertices.collect() === actualOGC.vertices.collect())
    assert(expectedOGC.edges.collect() === actualOGC.edges.collect())
    info("regular cases passed")

    //When interval is completely outside the graph
    val sliceInterval2 = (Interval(LocalDate.parse("2001-01-01"), LocalDate.parse("2003-01-01")))
    val actualOGC2 = ogc.slice(sliceInterval2)
    assert(actualOGC2.vertices.collect() === OneGraphColumn.emptyGraph().vertices.collect())
    assert(actualOGC2.edges.collect() === OneGraphColumn.emptyGraph().edges.collect())
    info("interval completely outside the graph passed")

    //When the graph is empty
    val actualOGC3 = OneGraphColumn.emptyGraph().slice(sliceInterval2)
    assert(actualOGC3.vertices.collect() === OneGraphColumn.emptyGraph().vertices.collect())
    assert(actualOGC3.edges.collect() === OneGraphColumn.emptyGraph().edges.collect())
    info("empty graph passed")
  }

  test("temporal select function"){
    //Regular cases
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 22)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 22)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22))
    ))
    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22))
    ))
    val expectedOGC = OneGraphColumn.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER )
    var selectFunction = (x:Interval) => x.equals(Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")))
    var actualOGC = OGC.select(selectFunction, selectFunction)

    assert(expectedOGC.vertices.collect() === actualOGC.vertices.collect())
    assert(expectedOGC.edges.collect() === actualOGC.edges.collect())
    info("regular cases passed")

    //When interval is completely outside the graph
    selectFunction = (x:Interval) => x.equals(Interval(LocalDate.parse("2001-01-01"), LocalDate.parse("2003-01-01")))
    val actualOGC2 = OGC.select(selectFunction, selectFunction)
    assert(actualOGC2.vertices.collect() === OneGraphColumn.emptyGraph().vertices.collect())
    assert(actualOGC2.edges.collect() === OneGraphColumn.emptyGraph().edges.collect())
    info("interval completely outside the graph passed")

    //When the graph is empty
    val actualOGC3 = OneGraphColumn.emptyGraph().select(selectFunction, selectFunction)
    assert(actualOGC3.vertices.collect() === OneGraphColumn.emptyGraph().vertices.collect())
    assert(actualOGC3.edges.collect() === OneGraphColumn.emptyGraph().edges.collect())
    info("empty graph passed")
  }

  test("structural select function - epred"){
    //Regular cases
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22))
    ))
    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke"))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42))
    ))
    val expectedOGC = OneGraphColumn.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER )
    var actualOGC = OGC.select(epred = (ids, attrs) => ids._1 > 2 && attrs._2 == 42)

    assert(expectedOGC.vertices.collect() === actualOGC.vertices.collect())
    assert(expectedOGC.edges.collect() === actualOGC.edges.collect())
  }

  test("structural select function - vpred"){
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22))
    ))
    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro"))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42))
    ))
    val expectedOGC = OneGraphColumn.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER )
    var actualOGC = OGC.select(vpred = (id, attrs) => id > 3 && attrs._2 != "Ke")

    assert(expectedOGC.vertices.collect() === actualOGC.vertices.collect())
    assert(expectedOGC.edges.collect() === actualOGC.edges.collect())
  }

  test("structural select function - vpred and epred") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22))
    ))
    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro"))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42))
    ))
    val expectedOGC = OneGraphColumn.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)
    var actualOGC = OGC.select(vpred = (id, attrs) => id > 3 && attrs._2 != "Ke", epred = (ids, attrs) => ids._1 > 2 && attrs._2 == 42)

    assert(expectedOGC.vertices.collect() === actualOGC.vertices.collect())
    assert(expectedOGC.edges.collect() === actualOGC.edges.collect())
  }

  test("getSnapshot function") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22))
    ))
    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedUsers = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (3L, "Ron"),
      (4L, "Julia"),
      (5L, "Vera"),
      (6L, "Halima")
    ))
    val expectedEdges = ProgramContext.sc.parallelize(Array(
      Edge(1L, 4L, 42),
      Edge(3L, 5L, 42)
    ))
    var actualOGC = OGC.getSnapshot((LocalDate.parse("2012-07-01")))

    assert(expectedUsers.collect.toSet === actualOGC.vertices.collect.toSet)
    assert(expectedEdges.collect.toSet === actualOGC.edges.collect.toSet)
  }

  test("aggregateByTime -w/o structural") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2018-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)),
      ((4L, 6L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((4L, 6L), (Interval(LocalDate.parse("2012-06-01"), LocalDate.parse("2013-01-01")), 72))

    ))
    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resolution1Month = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2011-02-01"))
    val resolution3Years = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2014-01-01"))

    val actualOGC = OGC.aggregate(new TimeSpec(resolution3Years), Always(), Always(), (attr1, attr2) => attr2, (attr1, attr2) => attr2)()

    val expectedVertices : RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "John")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2012-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2018-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Mike"))
    ))

    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42))
    ))

    assert(expectedVertices.collect().toSet === actualOGC.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualOGC.edges.collect().toSet)

    val actualOGC2 = OGC.aggregate(new TimeSpec(resolution3Years), Always(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => Math.max(attr1, attr2))()

    val expectedVertices2 : RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "John")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2012-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2018-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Mike"))
    ))

    val expectedEdges2: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((4L, 6L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 72))
    ))

    assert(expectedVertices2.collect().toSet === actualOGC2.vertices.collect().toSet)
    assert(expectedEdges2.collect().toSet === actualOGC2.edges.collect().toSet)
  }

  test("aggregateByTime -with structural") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2018-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke")),
      (6L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Halima"))

    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)),
      ((4L, 6L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((4L, 6L), (Interval(LocalDate.parse("2012-06-01"), LocalDate.parse("2013-01-01")), 72)),
      ((2L, 4L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), 22)),
      ((2L, 4L), (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2017-06-01")), 72))
    ))

    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resolution1Month = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2011-02-01"))
    val resolution3Years = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2014-01-01"))
    val longerString = (a:String, b:String) =>
      if(a.length > b.length) a else if(a.length < b.length) b else if(a.compareTo(b) > 0) a else b


    val actualOGC = OGC.aggregate(new TimeSpec(resolution3Years), Always(), Always(), (name1, name2) => longerString(name1, name2), (count1, count2) => Math.max(count1, count2))((vid, attr1) => if(attr1.length < 5) 1L else 2L)

    val expectedVertices: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Lovro")),
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (2L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))
    ))

    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42))
    ))

    assert(expectedVertices.collect().toSet === actualOGC.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualOGC.edges.collect().toSet)

    val actualOGC2 = OGC.aggregate(new TimeSpec(resolution3Years) , Always(), Exists(), (name1, name2) => longerString(name1, name2), (count1, count2) => Math.max(count1, count2))((vid, name) => if(name.length < 5) 1L else 2L)

    val expectedVertices2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Lovro")),
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (2L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))
    ))


    val expectedEdges2: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 1L), (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((1L, 1L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 22)),
      ((1L, 2L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 72)),
      ((2L, 2L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 1L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((2L, 2L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 72))
    ))

    assert(expectedVertices2.collect().toSet === actualOGC2.vertices.collect.toSet)
    assert(expectedEdges2.collect().toSet === actualOGC2.edges.collect().toSet)
  }


  test("aggregateByChange -w/o structural") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)),
      ((4L, 6L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((4L, 6L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), 72))
    ))

    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)
    val actualOGC = OGC.aggregate(new ChangeSpec(1), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)()

    assert(users.collect().toSet === actualOGC.vertices.collect().toSet)
    assert(edges.collect().toSet === actualOGC.edges.collect().toSet)

    val actualOGC2 = OGC.aggregate(new ChangeSpec(2), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)()
    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), "Ke"))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2017-01-01")), 22)),
      ((5L, 7L), (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), 22)),
      ((4L, 8L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), 42)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((4L, 6L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((4L, 6L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2013-01-01")), 72))
    ))

    assert(expectedUsers.collect().toSet === actualOGC2.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualOGC2.edges.collect().toSet)

    val actualOGC3 = OGC.aggregate(new ChangeSpec(2), Always(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)()

    val expectedUsers3: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2013-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana"))
    ))
    val expectedEdges3: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), 22)),
      ((4L, 6L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 22))
    ))

    assert(expectedUsers3.collect().toSet === actualOGC3.vertices.collect().toSet)
    assert(expectedEdges3.collect().toSet === actualOGC3.edges.collect().toSet)
  }

  test("aggregateByChange -with structural"){
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2018-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke"))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)),
      ((4L, 6L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((4L, 6L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), 72))
    ))

    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val longerString = (a:String, b:String) =>
      if(a.length > b.length) a else if(a.length < b.length) b else if(a.compareTo(b) > 0) a else b

    val actualOGC = OGC.aggregate(new ChangeSpec(3), Always(), Always(), (name1, name2) => longerString(name1, name2), (attr1, attr2) => Math.max(attr1, attr2))((vid, attr1) => if(attr1.length < 5) 1L else 2L)

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Lovro")),
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (2L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))
    ))

    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((2L, 2L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 72))
    ))

    assert(expectedUsers.collect().toSet === actualOGC.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualOGC.edges.collect().toSet)

    val actualOGC2 = OGC.aggregate(new ChangeSpec(3), Always(), Exists(), (name1, name2) => longerString(name1, name2), (attr1, attr2) => Math.max(attr1, attr2))((vid, attr1) => if(attr1.length < 5) 1L else 2L)

    val expectedUsers2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Lovro")),
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (2L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))
    ))

    val expectedEdges2: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 1L), (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((1L, 1L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 22)),
      ((1L, 2L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((2L, 2L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 2L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 72)),
      ((2L, 1L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22))
    ))

    assert(expectedUsers2.collect().toSet === actualOGC2.vertices.collect().toSet)
    assert(expectedEdges2.collect().toSet === actualOGC2.edges.collect().toSet)

  }

  test("aggregateByChange -with structural only"){
    val users: RDD[(VertexId, (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), null)),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), null)),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), null)),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), null)),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), null)),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), null)),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), null)),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), null)),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), null))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), null)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), null)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), null)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), null)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), null)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), null)),
      ((4L, 6L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), null))
    ))

    val OGC = OneGraphColumn.fromRDDs(users, edges, null, StorageLevel.MEMORY_ONLY_SER)
    val actualOGC = OGC.aggregate(new ChangeSpec(1), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)()

    assert(users.collect().toSet === actualOGC.vertices.collect().toSet)
    assert(edges.collect().toSet === actualOGC.edges.collect().toSet)

    val actualOGC2 = OGC.aggregate(new ChangeSpec(2), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)()
    val expectedUsers: RDD[(VertexId, (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2017-01-01")), null)),
      (2L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), null)),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), null)),
      (4L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2017-01-01")), null)),
      (5L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), null)),
      (6L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), null)),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), null)),
      (8L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), null)),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), null))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), null)),
      ((3L, 5L), (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2013-01-01")), null)),
      ((1L, 2L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2017-01-01")), null)),
      ((5L, 7L), (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), null)),
      ((4L, 8L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), null)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), null)),
      ((4L, 6L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), null))
    ))

    assert(expectedUsers.collect().toSet === actualOGC2.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualOGC2.edges.collect().toSet)

    val actualOGC3 = OGC.aggregate(new ChangeSpec(2), Always(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)()

    val expectedUsers3: RDD[(VertexId, (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2017-01-01")), null)),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), null)),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2013-01-01")), null)),
      (4L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2017-01-01")), null)),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), null)),
      (6L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), null)),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), null))
    ))
    val expectedEdges3: RDD[((VertexId, VertexId), (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), null)),
      ((3L, 5L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2013-01-01")), null)),
      ((1L, 2L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), null)),
      ((4L, 6L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), null))
    ))

    assert(expectedUsers3.collect().toSet === actualOGC3.vertices.collect().toSet)
    assert(expectedEdges3.collect().toSet === actualOGC3.edges.collect().toSet)
  }

  test("size, getTemporalSequence") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2012-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22))
    ))
    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resultInterval = OGC.size()
    val expectedInterval = Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2018-01-01"))
    assert(resultInterval === expectedInterval)

    val resultSeq = OGC.getTemporalSequence

    val expectedSequence = Seq(
      Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2010-01-01")),
      Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2012-01-01")),
      Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")),
      Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")),
      Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")),
      Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-01-01"))
    )
    assert(resultSeq === expectedSequence)

  }

  test("degree") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((1L, 2L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42))
    ))
    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resultDegree = OGC.degree

    val expectedDegree = ProgramContext.sc.parallelize(Array(
      (1L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")),1)),
      (1L,(Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")),2)),
      (1L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2017-01-01")),1)),
      (2L,(Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2017-01-01")),1)),
      (3L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),1))
    ))

    assert(expectedDegree.collect.toSet === resultDegree.collect.toSet)
  }


  test("Union and Intersection") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "c")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "e"))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((3L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((4L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42))
    ))

    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "A")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), "C")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d1")),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), "E"))
    ))

    val edges2: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 22)),
      ((2L, 3L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 52)),
      ((3L, 3L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), 22)),
      ((4L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 52)),
      ((5L, 5L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), 22))
    ))

    val OGC2 = OneGraphColumn.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),"a")),
      (1L,(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),"A")),
      (2L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),"b")),
      (2L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),"b1")),
      (3L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),"c")),
      (3L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),"C")),
      (4L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),"d1")),
      (5L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),"e"))
    ))

    val expectedEdgesUnion: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L,2L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),42)),
      ((1L,2L),(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),22)),
      ((2L,3L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),42)),
      ((2L,3L),(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),52)),
      ((3L,3L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),42)),
      ((3L,3L),(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),22)),
      ((4L,4L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),52)),
      ((2L,5L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),42)),
      ((5L,5L),(Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")),22))
    ))


    val resultOGCUnion = OGC.union(OGC2, (name1, name2) =>  if(name1.compareTo(name2) > 0) name1 else name2, (a, b) => Math.max(a,b) )

    assert(resultOGCUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultOGCUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)

    val resultOGCIntersection = OGC.intersection(OGC2, (name1, name2) =>  if(name1.compareTo(name2) > 0) name1 else name2, (a, b) => Math.max(a,b) )

    val expectedVerticesIntersection: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (3L,(Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")),"c")),
      (4L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),"d1")),
      (5L,(Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")),"e"))
    ))

    val expectedEdgesIntersection: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((3L,3L),(Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")),42)),
      ((4L,4L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),52))
    ))

    assert(resultOGCIntersection.vertices.collect.toSet === expectedVerticesIntersection.collect.toSet)
    assert(resultOGCIntersection.edges.collect.toSet === expectedEdgesIntersection.collect.toSet)
  }

  test("Union and intersection -when there is no overlap between two graphs"){
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b"))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42))
    ))

    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))

    val edges2: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((2L, 3L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52))
    ))

    val OGC2 = OneGraphColumn.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),"a")),
      (2L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),"b")),
      (2L,(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),"b1")),
      (3L,(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),"C"))
    ))

    val expectedEdgesUnion: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L,2L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),42)),
      ((2L,3L),(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),52))
    ))

    val resultOGCUnion = OGC.union(OGC2, (name1, name2) =>  if(name1.compareTo(name2) > 0) name1 else name2, (a, b) => Math.max(a,b) )

    assert(resultOGCUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultOGCUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)

    val resultOGCIntersection = OGC.intersection(OGC2, (name1, name2) => if (name1.compareTo(name2) > 0) name1 else name2, (a, b) => Math.max(a, b))

    assert(resultOGCIntersection.vertices.collect.toSet === OneGraphColumn.emptyGraph().vertices.collect.toSet)
    assert(resultOGCIntersection.edges.collect.toSet === OneGraphColumn.emptyGraph().edges.collect.toSet)
  }

  test("Union and intersection -when graph.span.start == graph2.span.end"){
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b"))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42))
    ))

    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),"a")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))

    val edges2: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((2L, 3L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52))
    ))

    val OGC2 = OneGraphColumn.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),"a")),
      (2L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),"b")),
      (2L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),"b1")),
      (3L,(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),"C"))
    ))

    val expectedEdgesUnion: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L,2L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),42)),
      ((2L,3L),(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),52))
    ))

    val resultOGCUnion = OGC.union(OGC2, (name1, name2) =>  if(name1.compareTo(name2) > 0) name1 else name2, (a, b) => Math.max(a,b) )

    assert(resultOGCUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultOGCUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)

    val resultOGCIntersection = OGC.intersection(OGC2, (name1, name2) => if (name1.compareTo(name2) > 0) name1 else name2, (a, b) => Math.max(a, b))

    assert(resultOGCIntersection.vertices.collect.toSet === OneGraphColumn.emptyGraph().vertices.collect.toSet)
    assert(resultOGCIntersection.edges.collect.toSet === OneGraphColumn.emptyGraph().edges.collect.toSet)
  }

  test("Union and Intersection - with Null") {
    val users: RDD[(VertexId, (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null)),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null)),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null)),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null)),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null)),
      ((2L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null)),
      ((3L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null)),
      ((4L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null)),
      ((2L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null))
    ))

    val OGC = OneGraphColumn.fromRDDs(users, edges, null, StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), null)),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), null)),
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), null)),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null)),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), null))
    ))

    val edges2: RDD[((VertexId, VertexId), (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), null)),
      ((2L, 3L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), null)),
      ((3L, 3L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), null)),
      ((4L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null)),
      ((5L, 5L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), null))
    ))

    val OGC2 = OneGraphColumn.fromRDDs(users2, edges2, null, StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      (1L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),null)),
      (1L,(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),null)),
      (2L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),null)),
      (3L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),null)),
      (4L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),null)),
      (5L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),null))
    ))

    val expectedEdgesUnion: RDD[((VertexId, VertexId), (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      ((1L,2L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),null)),
      ((1L,2L),(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),null)),
      ((2L,3L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),null)),
      ((3L,3L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),null)),
      ((4L,4L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),null)),
      ((2L,5L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),null)),
      ((5L,5L),(Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")),null))
    ))


    val resultOGCUnion = OGC.union(OGC2, (a, b) => a, (a, b) => a)

    assert(resultOGCUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultOGCUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)

    val resultOGCIntersection = OGC.intersection(OGC2, (a, b) => a, (a, b) => a)

    val expectedVerticesIntersection: RDD[(VertexId, (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      (3L,(Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")),null)),
      (4L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),null)),
      (5L,(Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")),null))
    ))

    val expectedEdgesIntersection: RDD[((VertexId, VertexId), (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      ((3L,3L),(Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")),null)),
      ((4L,4L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),null))
    ))

    assert(resultOGCIntersection.vertices.collect.toSet === expectedVerticesIntersection.collect.toSet)
    assert(resultOGCIntersection.edges.collect.toSet === expectedEdgesIntersection.collect.toSet)

  }

  test("Union and intersection -when there is no overlap between two graphs and has null attributes"){
    val users: RDD[(VertexId, (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null)),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null))
    ))

    val OGC = OneGraphColumn.fromRDDs(users, edges, null, StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), null)),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), null))
    ))

    val edges2: RDD[((VertexId, VertexId), (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      ((2L, 3L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), null))
    ))

    val OGC2 = OneGraphColumn.fromRDDs(users2, edges2, null, StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      (1L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),null)),
      (2L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),null)),
      (2L,(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),null)),
      (3L,(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),null))
    ))

    val expectedEdgesUnion: RDD[((VertexId, VertexId), (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      ((1L,2L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),null)),
      ((2L,3L),(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),null))
    ))

    val resultOGCUnion = OGC.union(OGC2, (a, b) =>  a, (a, b) => a)

    assert(resultOGCUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultOGCUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)

    val resultOGCIntersection = OGC.intersection(OGC2, (a, b) => a, (a, b) => a)

    assert(resultOGCIntersection.vertices.collect.toSet === OneGraphColumn.emptyGraph().vertices.collect.toSet)
    assert(resultOGCIntersection.edges.collect.toSet === OneGraphColumn.emptyGraph().edges.collect.toSet)
  }

  test("Union and intersection -when graph.span.start == graph2.span.end and has null attributes"){
    val users: RDD[(VertexId, (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null)),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), null))
    ))

    val OGC = OneGraphColumn.fromRDDs(users, edges, null, StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), null)),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), null)),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), null))
    ))

    val edges2: RDD[((VertexId, VertexId), (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      ((2L, 3L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), null))
    ))

    val OGC2 = OneGraphColumn.fromRDDs(users2, edges2, null, StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      (1L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),null)),
      (2L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),null)),
      (3L,(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),null))
    ))

    val expectedEdgesUnion: RDD[((VertexId, VertexId), (Interval, Null))] = ProgramContext.sc.parallelize(Array(
      ((1L,2L),(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),null)),
      ((2L,3L),(Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),null))
    ))

    val resultOGCUnion = OGC.union(OGC2, (a, b) =>  a, (a, b) => a)

    assert(resultOGCUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultOGCUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)

    val resultOGCIntersection = OGC.intersection(OGC2, (a, b) =>  a, (a, b) => a)

    assert(resultOGCIntersection.vertices.collect.toSet === OneGraphColumn.emptyGraph().vertices.collect.toSet)
    assert(resultOGCIntersection.edges.collect.toSet === OneGraphColumn.emptyGraph().edges.collect.toSet)
  }

  test("Project") {
    //Checks for projection and coalescing of vertices and edges
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "B")),
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), "b")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "c")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2012-01-01")), 4)),
      ((1L, 2L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), -4)),
      ((1L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 2))
    ))
    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val actualOGC = OGC.project(edge => (edge.attr*edge.attr) , (vertex, name) => name.toUpperCase, "Default")

    val expectedVertices: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2016-01-01")), "B")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "C")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "D"))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 16)),
      ((1L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 4))
    ))

    assert(expectedVertices.collect().toSet === actualOGC.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualOGC.edges.collect().toSet)
  }

  test("verticesAggregated and edgesAggregated functions"){
    val vertices: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2022-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (2L, (Interval(LocalDate.parse("2018-02-01"), LocalDate.parse("2020-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2019-01-01"), LocalDate.parse("2022-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2006-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (4L, (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2019-07-14")), "Vera"))
    ))

    val expectedVertices: RDD[(VertexId, Map[Interval, String])] = ProgramContext.sc.parallelize(Array(
      (1L, Map(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2022-01-01")) -> "John")),
      (2L, Map(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")) -> "Mike", Interval(LocalDate.parse("2018-02-01"), LocalDate.parse("2020-01-01")) ->"Mike")),
      (3L, Map(Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")) -> "Ron", Interval(LocalDate.parse("2019-01-01"), LocalDate.parse("2022-01-01")) -> "Ron")),
      (4L, Map(Interval(LocalDate.parse("2006-01-01"), LocalDate.parse("2017-01-01")) -> "Julia", Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2019-07-14")) -> "Vera" ))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22)),
      ((1L, 4L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), 56)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)),
      ((1L, 4L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 12)),
      ((3L, 5L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), 42))
    ))

    val expectedEdges: RDD[((VertexId,VertexId),Map[Interval, Int])] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), Map(Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")) -> 42, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")) -> 12, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")) -> 56)),
      ((3L, 5L), Map(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")) -> 42, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")) -> 42)),
      ((1L, 2L), Map(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")) -> 22)),
      ((5L, 7L), Map(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")) -> 22)),
      ((4L, 8L), Map(Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")) -> 42))
    ))

    val actualOGC = OneGraphColumn.fromRDDs(vertices, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

    assert(actualOGC.verticesAggregated.collect.toSet === expectedVertices.collect.toSet)
    assert(actualOGC.edgesAggregated.collect.toSet === expectedEdges.collect.toSet)
  }

  test("from RDD") {
    //Checks if the fromRDD function creates the correct graphs. Graphs variable is protected so to get the graphs, we use getSnapshot function
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2012-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22))
    ))
    val OGC = OneGraphColumn.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    //should be empty
    val graph1 = OGC.getSnapshot(LocalDate.parse("2008-12-01"))
    val graph2 = OGC.getSnapshot(LocalDate.parse("2018-01-01"))

    assert(graph1.vertices.isEmpty())
    assert(graph1.edges.isEmpty())

    assert(graph2.vertices.isEmpty())
    assert(graph2.edges.isEmpty())

    //not empty
    val graph3 = OGC.getSnapshot(LocalDate.parse("2009-01-01"))
    val expectedUsers3 = ProgramContext.sc.parallelize(Array(
      (3L, "Ron")
    ))
    assert(expectedUsers3.collect.toSet === graph3.vertices.collect.toSet)
    assert(graph3.edges.isEmpty())

    val graph4 = OGC.getSnapshot(LocalDate.parse("2010-01-01"))
    val expectedUsers4 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (3L, "Ron")
    ))
    val expectedEdges4 = ProgramContext.sc.parallelize(Array(
      Edge(1L, 3L, 42)
    ))
    assert(expectedUsers4.collect.toSet === graph4.vertices.collect.toSet)
    assert(expectedEdges4.collect.toSet === graph4.edges.collect.toSet)

    val graph5 = OGC.getSnapshot(LocalDate.parse("2012-01-01"))
    val expectedUsers5 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (3L, "Ron")
    ))
    assert(expectedUsers5.collect.toSet === graph5.vertices.collect.toSet)
    assert(graph5.edges.isEmpty())

    val graph6 = OGC.getSnapshot(LocalDate.parse("2014-01-01"))
    val expectedUsers6 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (2L, "Mike")
    ))
    val expectedEdges6 = ProgramContext.sc.parallelize(Array(
      Edge(1L, 2L, 22)
    ))
    assert(expectedUsers6.collect.toSet === graph6.vertices.collect.toSet)
    assert(expectedUsers6.collect.toSet === graph6.vertices.collect.toSet)

    val graph7 = OGC.getSnapshot(LocalDate.parse("2016-01-01"))
    val expectedUsers7 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (2L, "Mike")
    ))
    assert(expectedUsers7.collect.toSet === graph7.vertices.collect.toSet)
    assert(graph7.edges.isEmpty())

    val graph8 = OGC.getSnapshot(LocalDate.parse("2017-01-01"))
    val expectedUsers8 = ProgramContext.sc.parallelize(Array(
      (2L, "Mike")
    ))
    assert(expectedUsers8.collect.toSet === graph8.vertices.collect.toSet)
    assert(graph8.edges.isEmpty())
  }

  test("connected components"){
    val nodes: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Lovro"))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((3L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((4L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((5L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((7L, 8L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),

      //second representative graph
      ((1L, 3L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((1L, 5L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((3L, 7L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((5L, 7L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 4L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 6L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((4L, 8L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((6L, 8L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42))
    ))

    val expectedNodes:RDD[(VertexId, (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      (1L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),1)),
      (2L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),1)),
      (2L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),2)),
      (3L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),1)),
      (4L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),1)),
      (4L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),2)),
      (5L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),1)),
      (6L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),1)),
      (6L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),2)),
      (7L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),7)),
      (7L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),1)),
      (8L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),7)),
      (8L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),2))
    ))


    val OGC = OneGraphColumn.fromRDDs(nodes, edges, "Default")

    val actualOGC = OGC.connectedComponents()
    assert(actualOGC.vertices.collect.toSet == expectedNodes.collect.toSet)

  }

  test("undirected shortestPath") {
    val nodes: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Lovro"))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((3L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((4L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((5L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((7L, 8L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),

      //second representative graph
      ((1L, 3L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((1L, 5L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((3L, 7L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((5L, 7L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 4L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 6L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42))
    ))

    val expectedNodes:RDD[(VertexId, (Interval, Map[VertexId, Int]))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Map(1L->0, 2L->1))),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Map(1L->1, 2L->0))),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Map(1L->2, 2L->1))),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Map(1L->2, 2L->1))),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Map(1L->3, 2L->2))),
      (6L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Map(1L->2, 2L->1))),
      (7L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Map().asInstanceOf[Map[VertexId, Int]])),
      (8L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), Map().asInstanceOf[Map[VertexId, Int]])),

      //second representative graph
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Map(1L->0))),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Map(2L->0))),
      (3L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Map(1L->1))),
      (4L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Map(2L->1))),
      (5L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Map(1L->1))),
      (6L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Map(2L->1))),
      (7L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Map((1L->2))))
    ))

    val OGC = OneGraphColumn.fromRDDs(nodes, edges, "Default")

    val actualOGC = OGC.shortestPaths(false, Seq(1L, 2L))

    assert(actualOGC.vertices.collect.toSet == expectedNodes.collect.toSet)
  }

  ignore("directed shortestPath") {
    val nodes: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Lovro"))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((3L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((4L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((5L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((7L, 8L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),

      //second representative graph
      ((1L, 3L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((1L, 5L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((3L, 7L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((5L, 7L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 4L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 6L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42))
    ))

    val expectedNodes:RDD[(VertexId, (Interval, Map[VertexId, Int]))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Map(5L->3, 6L->2))),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Map(5L->2, 6L->1))),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Map(5L->1, 6L->2))),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Map(5L->1, 6L->2))),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Map(5L->0, 6L->1))),
      (6L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), Map(6L->0))),
      (7L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), Map().asInstanceOf[Map[VertexId, Int]])),
      (8L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), Map().asInstanceOf[Map[VertexId, Int]])),

      //second representative graph
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Map(5L->1))),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Map(6L->1))),
      (3L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Map().asInstanceOf[Map[VertexId, Int]])),
      (4L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Map().asInstanceOf[Map[VertexId, Int]])),
      (5L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Map(5L->0)))
    ))

    val OGC = OneGraphColumn.fromRDDs(nodes, edges, "Default")

    val actualOGC = OGC.shortestPaths(true, Seq(5L, 6L))
    assert(actualOGC.vertices.collect.toSet == expectedNodes.collect.toSet)
  }

  ignore("directed pagerank") {
    val nodes: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "Lovro"))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((3L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((4L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((5L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((7L, 8L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),

      //second representative graph
      ((1L, 3L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((1L, 5L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((3L, 7L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((5L, 7L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 4L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 6L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((4L, 8L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((6L, 8L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42))
    ))


    //PageRank for each representative graph was calculated by creating graph in  graphX and using spark's pagerank
    //The pagerank values are used in assert statement
    // Uncomment code below to execute spark's page rank
    //    val testNodes: RDD[(VertexId, String)] = ProgramContext.sc.parallelize(Array(
    //      (1L,  "John"),
    //      (2L,  "Mike"),
    //      (3L,  "Ron"),
    //      (4L,  "Julia"),
    //      (5L,  "Vera"),
    //      (6L,  "Halima"),
    //      (7L,  "Sanjana"),
    //      (8L,  "Lovro")
    //    ))
    //    val testEdges: RDD[Edge[Int]] = ProgramContext.sc.parallelize(Array(
    //      Edge(1L, 2L, 42),
    //      Edge(2L, 3L, 42),
    //      Edge(2L, 6L, 42),
    //      Edge(2L, 4L, 42),
    //      Edge(3L, 5L, 42),
    //      Edge(3L, 4L, 42),
    //      Edge(4L, 5L, 42),
    //      Edge(5L, 6L, 42),
    //      Edge(7L, 8L, 42)
    //    ))
    //    val graph1 = Graph(testNodes, testEdges, "Default")
    //    val pageRank1 = graph1.staticPageRank(10, 0.15)
    //
    //    val testNodes2: RDD[(VertexId, String)] = ProgramContext.sc.parallelize(Array(
    //      (1L,  "John"),
    //      (2L,  "Mike"),
    //      (3L,  "Ron"),
    //      (4L,  "Julia"),
    //      (5L,  "Vera"),
    //      (6L,  "Halima"),
    //      (7L,  "Sanjana"),
    //      (8L,  "Lovro")
    //    ))
    //    val testEdges2: RDD[Edge[Int]] = ProgramContext.sc.parallelize(Array(
    //      Edge(1L, 3L, 42),
    //      Edge(1L, 5L, 42),
    //      Edge(3L, 7L, 42),
    //      Edge(5L, 7L, 42),
    //      Edge(2L, 4L, 42),
    //      Edge(2L, 6L, 42),
    //      Edge(4L, 8L, 42),
    //      Edge(6L, 8L, 42)
    //    ))
    //    val graph2 = Graph(testNodes2, testEdges2, "Default")
    //    val pageRank2 = graph2.staticPageRank(10, 0.15)
    //
    //    println("first graph, 2010 - 2014")
    //    pageRank1.vertices.foreach(println)
    //    println("second graph, 2014-2018")
    //    pageRank2.vertices.foreach(println)

    val OGC = HybridGraph.fromRDDs(nodes, edges, "Default")
    val actualOGC = OGC.pageRank(true, 0.001, 0.15, 10)

    val expectedNodes:RDD[(VertexId, (Interval, Double))] = ProgramContext.sc.parallelize(Array(
      (1L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),0.15)),
      (2L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),0.15)),
      (2L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),0.27749999999999997)),
      (3L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),0.21375)),
      (3L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),0.22862499999999997)),
      (4L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),0.21375)),
      (4L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),0.32579062499999994)),
      (5L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),0.21375)),
      (5L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),0.52408765625)),
      (6L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),0.21375)),
      (6L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),0.6740995078125)),
      (7L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),0.513375)),
      (7L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),0.15)),
      (8L,(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")),0.513375)),
      (8L,(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),0.27749999999999997))
    ))

    assert(actualOGC.vertices.collect().toSet === expectedNodes.collect().toSet)
  }



}
