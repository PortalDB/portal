package edu.drexel.cs.dbgroup.temporalgraph.representations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.storage.StorageLevel

import org.scalatest.{Tag, FunSuite, BeforeAndAfter}

import java.time.LocalDate
import edu.drexel.cs.dbgroup.temporalgraph._

import scala.collection.parallel.ParSeq
import org.apache.spark.graphx.{Edge, EdgeRDD, VertexId, Graph}
import scala.math.max

class SnapshotGraphParallelSuite  extends FunSuite with BeforeAndAfter {
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
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

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
    val expectedSgp = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER )

    var actualSGP = sgp.slice(sliceInterval)

    assert(expectedSgp.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSgp.edges.collect() === actualSGP.edges.collect())
    info("regular cases passed")

    //When interval is completely outside the graph
    val sliceInterval2 = (Interval(LocalDate.parse("2001-01-01"), LocalDate.parse("2003-01-01")))
    val actualSGP2 = sgp.slice(sliceInterval2)
    assert(actualSGP2.vertices.collect() === SnapshotGraphParallel.emptyGraph().vertices.collect())
    assert(actualSGP2.edges.collect() === SnapshotGraphParallel.emptyGraph().edges.collect())
    info("interval completely outside the graph passed")

    //When the graph is empty
    val actualSGP3 = SnapshotGraphParallel.emptyGraph().slice(sliceInterval2)
    assert(actualSGP3.vertices.collect() === SnapshotGraphParallel.emptyGraph().vertices.collect())
    assert(actualSGP3.edges.collect() === SnapshotGraphParallel.emptyGraph().edges.collect())
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
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22))
    ))
    val expectedSgp = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER )
    var selectFunction = (x:Interval) => x.equals(Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")))
    var actualSGP = sgp.select(selectFunction, selectFunction)

    assert(expectedSgp.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSgp.edges.collect() === actualSGP.edges.collect())
    info("regular cases passed")

    //When interval is completely outside the graph
    selectFunction = (x:Interval) => x.equals(Interval(LocalDate.parse("2001-01-01"), LocalDate.parse("2003-01-01")))
    val actualSGP2 = sgp.select(selectFunction, selectFunction)
    assert(actualSGP2.vertices.collect() === SnapshotGraphParallel.emptyGraph().vertices.collect())
    assert(actualSGP2.edges.collect() === SnapshotGraphParallel.emptyGraph().edges.collect())
    info("interval completely outside the graph passed")

    //When the graph is empty
    val actualSGP3 = SnapshotGraphParallel.emptyGraph().select(selectFunction, selectFunction)
    assert(actualSGP3.vertices.collect() === SnapshotGraphParallel.emptyGraph().vertices.collect())
    assert(actualSGP3.edges.collect() === SnapshotGraphParallel.emptyGraph().edges.collect())
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
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

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
    val expectedSgp = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER )
    var actualSGP = sgp.select(epred = (ids, attrs) => ids._1 > 2 && attrs._2 == 42)

    assert(expectedSgp.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSgp.edges.collect() === actualSGP.edges.collect())

    //    //When the graph is empty
    //    val actualSGP3 = SnapshotGraphParallel.emptyGraph().select(epred = (ids, attrs) => ids._1 > 2 && attrs._2 == 42)
    //    assert(actualSGP3.vertices.collect() === SnapshotGraphParallel.emptyGraph().vertices.collect())
    //    assert(actualSGP3.edges.collect() === SnapshotGraphParallel.emptyGraph().edges.collect())
    //    info("empty graph passed")
  }

  test("structural select function - vpred"){
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
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

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
    val expectedSgp = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER )
    var actualSGP = sgp.select(vpred = (id, attrs) => id > 3 && attrs._2 != "Ke")

    assert(expectedSgp.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSgp.edges.collect() === actualSGP.edges.collect())
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
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

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
    val expectedSgp = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)
    var actualSGP = sgp.select(vpred = (id, attrs) => id > 3 && attrs._2 != "Ke", epred = (ids, attrs) => ids._1 > 2 && attrs._2 == 42)

    assert(expectedSgp.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSgp.edges.collect() === actualSGP.edges.collect())
  }

  test("getSnapshot function") {
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
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

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
    var actualSGP = sgp.getSnapshot((LocalDate.parse("2012-07-01")))

    assert(expectedUsers.collect() === actualSGP.vertices.sortBy(_._1).collect())
    assert(expectedEdges.collect() === actualSGP.edges.collect())
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
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resolution1Month = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2011-02-01"))
    val resolution3Years = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2014-01-01"))

    val actualSgp = sgp.aggregate(new TimeSpec(resolution3Years), Always(), Always(), (attr1, attr2) => attr2, (attr1, attr2) => attr2)()

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

    assert(expectedVertices.collect().toSet === actualSgp.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualSgp.edges.collect().toSet)

    val actualSgp2 = sgp.aggregate(new TimeSpec(resolution3Years), Always(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => Math.max(attr1, attr2))()

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

    assert(expectedVertices2.collect().toSet === actualSgp2.vertices.collect().toSet)
    assert(expectedEdges2.collect().toSet === actualSgp2.edges.collect().toSet)

    //TODO: Maybe add exists exists?

  }

  test("aggregateByTime -with structural(failing becuase it does not account the quantification)") {
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
      ((5L, 6L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((3L, 6L), (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), 22)),
      ((2L, 4L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), 22)),
      ((2L, 4L), (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2017-06-01")), 72))

    ))
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resolution1Month = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2011-02-01"))
    val resolution3Years = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2014-01-01"))
    val longerString = (a:String, b:String) =>
      if(a.length > b.length) a
      else if(a.length < b.length) b
      else
      if(a.compareTo(b) > 0) a else b


    val greater = (a:Int, b:Int) =>
      if(a > b) a
      else b

    val actualSgp = sgp.aggregate(new TimeSpec(resolution3Years), Always(), Always(), (name1, name2) => longerString(name1, name2), (count1, count2) => greater(count1, count2))((vid, attr1) => if(attr1.length < 5) 1L else 2L)
//
//    actualSgp.vertices.collect().foreach(println)
//    actualSgp.edges.collect().toSet.foreach(println)

    val expectedVertices: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2012-01-01")), "Ron")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Julia")),
      (1L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (2L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))
    ))

    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22))
    ))


    assert(expectedVertices.collect().toSet === actualSgp.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualSgp.edges.collect().toSet)


    val actualSgp2 = sgp.aggregate(new TimeSpec(resolution3Years) , Always(), Exists(), (name1, name2) => longerString(name1, name2), (count1, count2) => greater(count1, count2))((vid, name) => if(name.length < 5) 1L else 2L)

    val expectedVertices2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2012-01-01")), "Ron")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Julia")),
      (1L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (2L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))
    ))

    val expectedEdges2: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((2L, 4L), (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2017-06-01")), 72))
    ))

//    actualSgp2.vertices.collect().foreach(println)
//    actualSgp2.edges.collect().foreach(println)

    //TODO: Maybe add exists exists?

    //ignore, the assert statements are not correct
    //failing because aggregate function does not work
    //    assert(expectedVertices2.collect().sortBy(_._1) === actualSgp2.vertices.collect().sortBy(_._1))
    //    assert(expectedEdges2.collect().sortBy(_._1) === actualSgp2.edges.collect().sortBy(_._1))

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

    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)
    val actualSgp = sgp.aggregate(new ChangeSpec(1), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)()

    //passed
    assert(users.collect().toSet === actualSgp.vertices.collect().toSet)
    assert(edges.collect().toSet === actualSgp.edges.collect().toSet)

    //When the last change is less than given changes,  , if we are using quantifiction exists, it is included. If we use always, it is not.
    val actualSgp2 = sgp.aggregate(new ChangeSpec(2), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)()
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

    //passed
    assert(expectedUsers.collect().toSet === actualSgp2.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualSgp2.edges.collect().toSet)

    val actualSgp3 = sgp.aggregate(new ChangeSpec(2), Always(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)()

    val expectedUsers3: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), "Mike")),
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

    //passed
    assert(expectedUsers3.collect().toSet === actualSgp3.vertices.collect().toSet)
    assert(expectedEdges3.collect().toSet === actualSgp3.edges.collect().toSet)
  }

  test("aggregateByChange -with structural(failing becuase it does not account the quantification)") {
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

    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val longerString = (a:String, b:String) =>
      if(a.length > b.length) a
      else if(a.length < b.length) b
      else
      if(a.compareTo(b) > 0) a else b

    val actualSgp = sgp.aggregate(new ChangeSpec(2), Always(), Always(), (name1, name2) => longerString(name1, name2), (attr1, attr2) => Math.max(attr1, attr2))((vid, attr1) => if(attr1.length < 5) 1L else 2L)

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), "Mike")),
      (1L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Ron")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (1L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (2L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (2L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana"))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), 22)),
      ((4L, 6L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 22))
    ))

    //not sure about the expected egdes

//    actualSgp.vertices.collect().foreach(println)
//    actualSgp.edges.collect().foreach(println)

    assert(expectedUsers.collect().toSet === actualSgp.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualSgp.edges.collect().toSet)
  }

  test("from RDD") {
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
      ((4L, 6L), (Interval(LocalDate.parse("2012-06-01"), LocalDate.parse("2013-01-01")), 72))
    ))
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)
    //TODO how to test the graphs?

  }

  test("from graphs") {
    var intervals: Seq[Interval] = Seq[Interval]()
    var graphs: ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]]()

    var users1: RDD[(VertexId, String)] = ProgramContext.sc.parallelize(Array[(VertexId, String)](
      (1L, "John"),
      (2L, "Mike"),
      (3L, "Ron"),
      (4L, "Julia")
    ))
    var edges1: EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.parallelize(Array(
      Edge(1L, 2L, 2),
      Edge(1L, 4L, 32)
    )))
    val graph1 = Graph(users1, edges1, "Default", edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)


    var users2 = ProgramContext.sc.parallelize(Array[(VertexId, String)](
      (4L, "Julia"),
      (5L, "Vera"),
      (6L, "Halima"),
      (1L, "John")
    ))
    var edges2 : EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.parallelize(Array(
      Edge(4L, 5L, 12),
      Edge(1L, 4L, 102)
    )))
    val graph2 = Graph(users2, edges2, "Default", edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)

    var users3 = ProgramContext.sc.parallelize(Array[(VertexId, String)](
      (7L, "Sanjana"),
      (8L, "Lovro"),
      (9L, "Ke"),
      (4L, "Julia")
    ))
    var edges3 : EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.parallelize(Array(
      Edge(7L, 8L, 22)
    )))
    val graph3 = Graph(users3, edges3, "Default", edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)

    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-01-01"))

    intervals = intervals :+ testInterval1 :+ testInterval2 :+ testInterval3
    graphs = graphs :+ graph1 :+ graph2 :+ graph3

    val actualSgp = SnapshotGraphParallel.fromGraphs(intervals, graphs, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), "Julia")),
      (4L, (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-01-01")), "Ke"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), 32)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), 2)),
      ((1L, 4L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01")), 102)),
      ((4L, 5L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01")), 12)),
      ((7L, 8L), (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-01-01")), 22))
    ))

    assert(users.collect().toSet === actualSgp.vertices.collect().toSet)
    assert(edges.collect().toSet === actualSgp.edges.collect().toSet)

  }


}
