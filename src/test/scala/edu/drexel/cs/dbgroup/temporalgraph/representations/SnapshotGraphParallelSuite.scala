package edu.drexel.cs.dbgroup.temporalgraph.representations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.scalatest.{BeforeAndAfter, FunSuite}

import java.time.LocalDate
import edu.drexel.cs.dbgroup.temporalgraph._

import scala.collection.parallel.ParSeq
import org.apache.spark.graphx.{Edge, EdgeRDD, VertexId, Graph}

import java.util.Map
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import collection.JavaConverters._

class SnapshotGraphParallelSuite extends FunSuite with BeforeAndAfter {
  before {
    if (ProgramContext.sc == null) {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      val conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME")).setMaster("local[2]")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val sc = new SparkContext(conf)
      ProgramContext.setContext(sc)
      println(" ") //the first line starts from between
    }
  }

  test("slice function") {
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
    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

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
    val expectedSGP = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)

    SGP.materialize
    var actualSGP = SGP.slice(sliceInterval)

    assert(expectedSGP.vertices.collect().toSet === actualSGP.vertices.collect().toSet)
    assert(expectedSGP.edges.collect().toSet === actualSGP.edges.collect().toSet)
    assert(expectedSGP.getTemporalSequence.collect === actualSGP.getTemporalSequence.collect)
    info("regular cases passed")

    //When interval is completely outside the graph
    val sliceInterval2 = (Interval(LocalDate.parse("2001-01-01"), LocalDate.parse("2003-01-01")))
    val actualSGP2 = SGP.slice(sliceInterval2)
    assert(actualSGP2.vertices.collect() === SnapshotGraphParallel.emptyGraph().vertices.collect())
    assert(actualSGP2.edges.collect() === SnapshotGraphParallel.emptyGraph().edges.collect())
    assert(actualSGP2.getTemporalSequence.collect === Seq[Interval]())
    info("interval completely outside the graph passed")

    //When the graph is empty
    val actualSGP3 = SnapshotGraphParallel.emptyGraph().slice(sliceInterval2)
    assert(actualSGP3.vertices.collect() === SnapshotGraphParallel.emptyGraph().vertices.collect())
    assert(actualSGP3.edges.collect() === SnapshotGraphParallel.emptyGraph().edges.collect())
    assert(actualSGP3.getTemporalSequence.collect === Seq[Interval]())
    info("empty graph passed")
  }

  test("temporal select function") {
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
    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22))
    ))
    val expectedSGP = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)
    var selectFunction = (x: Interval) => x.equals(Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")))
    var actualSGP = SGP.select(selectFunction, selectFunction)

    assert(expectedSGP.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSGP.edges.collect() === actualSGP.edges.collect())
    assert(expectedSGP.getTemporalSequence.collect === actualSGP.getTemporalSequence.collect)
    info("regular cases passed")

    //When interval is completely outside the graph
    selectFunction = (x: Interval) => x.equals(Interval(LocalDate.parse("2001-01-01"), LocalDate.parse("2003-01-01")))
    val actualSGP2 = SGP.select(selectFunction, selectFunction)
    assert(actualSGP2.vertices.collect() === SnapshotGraphParallel.emptyGraph().vertices.collect())
    assert(actualSGP2.edges.collect() === SnapshotGraphParallel.emptyGraph().edges.collect())
    assert(actualSGP2.getTemporalSequence.collect === Seq[Interval]())
    info("interval completely outside the graph passed")

    //When the graph is empty
    val actualSGP3 = SnapshotGraphParallel.emptyGraph().select(selectFunction, selectFunction)
    assert(actualSGP3.vertices.collect() === SnapshotGraphParallel.emptyGraph().vertices.collect())
    assert(actualSGP3.edges.collect() === SnapshotGraphParallel.emptyGraph().edges.collect())
    assert(actualSGP3.getTemporalSequence.collect === Seq[Interval]())
    info("empty graph passed")
  }

  test("structural select function - epred") {
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
    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

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
    val expectedSGP = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)
    var actualSGP = SGP.select(epred = (ids, attrs) => ids._1 > 2 && attrs._2 == 42)

    assert(expectedSGP.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSGP.edges.collect() === actualSGP.edges.collect())
    assert(expectedSGP.getTemporalSequence.collect === actualSGP.getTemporalSequence.collect)
  }

  test("structural select function - vpred") {
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
    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

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
    val expectedSGP = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)
    var actualSGP = SGP.select(vpred = (id, attrs) => id > 3 && attrs._2 != "Ke")

    assert(expectedSGP.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSGP.edges.collect() === actualSGP.edges.collect())
    assert(expectedSGP.getTemporalSequence.collect === actualSGP.getTemporalSequence.collect)
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
    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

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
    val expectedSGP = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)
    var actualSGP = SGP.select(vpred = (id, attrs) => id > 3 && attrs._2 != "Ke", epred = (ids, attrs) => ids._1 > 2 && attrs._2 == 42)

    assert(expectedSGP.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSGP.edges.collect() === actualSGP.edges.collect())
    assert(expectedSGP.getTemporalSequence.collect === actualSGP.getTemporalSequence.collect)
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
    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

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
    var actualSGP = SGP.getSnapshot((LocalDate.parse("2012-07-01")))

    assert(expectedUsers.collect.toSet === actualSGP.vertices.collect.toSet)
    assert(expectedEdges.collect.toSet === actualSGP.edges.collect.toSet)
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
    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resolution1Month = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2011-02-01"))
    val resolution3Years = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2014-01-01"))

    val actualSGP = SGP.aggregate(new TimeSpec(resolution3Years), Always(), Always(), (attr1, attr2) => attr2, (attr1, attr2) => attr2)()

    val expectedVertices: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
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
    val expectedSGP = SnapshotGraphParallel.fromRDDs(expectedVertices, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedVertices.collect().toSet === actualSGP.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualSGP.edges.collect().toSet)
    assert(expectedSGP.getTemporalSequence.collect === actualSGP.getTemporalSequence.collect)

    val actualSGP2 = SGP.aggregate(new TimeSpec(resolution3Years), Always(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => Math.max(attr1, attr2))()

    val expectedVertices2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
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
    val expectedSGP2 = SnapshotGraphParallel.fromRDDs(expectedVertices2, expectedEdges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedVertices2.collect().toSet === actualSGP2.vertices.collect().toSet)
    assert(expectedEdges2.collect().toSet === actualSGP2.edges.collect().toSet)
    assert(expectedSGP2.getTemporalSequence.collect === actualSGP2.getTemporalSequence.collect)
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

    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resolution1Month = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2011-02-01"))
    val resolution3Years = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2014-01-01"))
    val longerString = (a: String, b: String) =>
      if (a.length > b.length) a else if (a.length < b.length) b else if (a.compareTo(b) > 0) a else b

    val actualSGP = SGP.aggregate(new TimeSpec(resolution3Years), Always(), Always(), (name1, name2) => longerString(name1, name2), (count1, count2) => Math.max(count1, count2))((vid, attr1) => if (attr1.length < 5) 1L else 2L)

    val expectedVertices: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Lovro")),
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (2L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))
    ))

    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42))
    ))
    val expectedSGP = SnapshotGraphParallel.fromRDDs(expectedVertices, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedVertices.collect().toSet === actualSGP.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualSGP.edges.collect().toSet)
    assert(expectedSGP.getTemporalSequence.collect === actualSGP.getTemporalSequence.collect)

    val actualSGP2 = SGP.aggregate(new TimeSpec(resolution3Years), Always(), Exists(), (name1, name2) => longerString(name1, name2), (count1, count2) => Math.max(count1, count2))((vid, name) => if (name.length < 5) 1L else 2L)

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
    val expectedSGP2 = SnapshotGraphParallel.fromRDDs(expectedVertices2, expectedEdges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedVertices2.collect().toSet === actualSGP2.vertices.collect.toSet)
    assert(expectedEdges2.collect().toSet === actualSGP2.edges.collect().toSet)
    assert(expectedSGP2.getTemporalSequence.collect === actualSGP2.getTemporalSequence.collect)
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

    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)
    val actualSGP = SGP.aggregate(new ChangeSpec(1), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)()
    val expectedSGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(users.collect().toSet === actualSGP.vertices.collect().toSet)
    assert(edges.collect().toSet === actualSGP.edges.collect().toSet)
    assert(expectedSGP.getTemporalSequence.collect === actualSGP.getTemporalSequence.collect)

    val actualSGP2 = SGP.aggregate(new ChangeSpec(2), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)()
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
    val expectedSGP2 = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedUsers.collect().toSet === actualSGP2.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualSGP2.edges.collect().toSet)
    assert(expectedSGP2.getTemporalSequence.collect === actualSGP2.getTemporalSequence.collect)

    val actualSGP3 = SGP.aggregate(new ChangeSpec(2), Always(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)()

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
    val expectedSGP3 = SnapshotGraphParallel.fromRDDs(expectedUsers3, expectedEdges3, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedUsers3.collect().toSet === actualSGP3.vertices.collect().toSet)
    assert(expectedEdges3.collect().toSet === actualSGP3.edges.collect().toSet)
    assert(expectedSGP3.getTemporalSequence.collect === actualSGP3.getTemporalSequence.collect)

  }

  test("aggregateByChange -with structural") {
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

    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val longerString = (a: String, b: String) =>
      if (a.length > b.length) a else if (a.length < b.length) b else if (a.compareTo(b) > 0) a else b

    val actualSGP = SGP.aggregate(new ChangeSpec(3), Always(), Always(), (name1, name2) => longerString(name1, name2), (attr1, attr2) => Math.max(attr1, attr2))((vid, attr1) => if (attr1.length < 5) 1L else 2L)

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
    val expectedSGP = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedUsers.collect().toSet === actualSGP.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualSGP.edges.collect().toSet)
    assert(expectedSGP.getTemporalSequence.collect === actualSGP.getTemporalSequence.collect)

    val actualSGP2 = SGP.aggregate(new ChangeSpec(3), Always(), Exists(), (name1, name2) => longerString(name1, name2), (attr1, attr2) => Math.max(attr1, attr2))((vid, attr1) => if (attr1.length < 5) 1L else 2L)

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
    val expectedSGP2 = SnapshotGraphParallel.fromRDDs(expectedUsers2, expectedEdges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedUsers2.collect().toSet === actualSGP2.vertices.collect().toSet)
    assert(expectedEdges2.collect().toSet === actualSGP2.edges.collect().toSet)
    assert(expectedSGP2.getTemporalSequence.collect === actualSGP2.getTemporalSequence.collect)
  }

  test("size, getTemporalSequence.collect") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2012-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22))
    ))
    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resultInterval = SGP.size()
    val expectedInterval = Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2018-01-01"))
    assert(resultInterval === expectedInterval)

    val resultSeq = SGP.getTemporalSequence.collect

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
    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resultDegree = SGP.degree

    val expectedDegree = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 1)),
      (1L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 2)),
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2017-01-01")), 1)),
      (2L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2017-01-01")), 1)),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 1))
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

    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

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

    val SGP2 = SnapshotGraphParallel.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, Set[String]))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set("a"))),
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), Set("A"))),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set("b"))),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Set("b1"))),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), Set("c"))),
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), Set("c", "C"))),
      (3L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Set("C"))),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set("d", "d1"))),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), Set("e"))),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), Set("e", "E"))),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), Set("e")))
    ))

    val expectedEdgesUnion: RDD[((VertexId, VertexId), (Interval, Set[Int]))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set(42))),
      ((1L, 2L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), Set(22))),
      ((2L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set(42))),
      ((2L, 3L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Set(52))),
      ((3L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), Set(42))),
      ((3L, 3L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), Set(42, 22))),
      ((3L, 3L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Set(22))),
      ((4L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set(42, 52))),
      ((2L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set(42))),
      ((5L, 5L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), Set(22)))
    ))

    val resultSGPUnion = SGP.union(SGP2)
    val expectedSGPUnion = SnapshotGraphParallel.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, Set("Default"), StorageLevel.MEMORY_ONLY_SER)
    val test = resultSGPUnion.vertices
    assert(resultSGPUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultSGPUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultSGPUnion.getTemporalSequence.collect === expectedSGPUnion.getTemporalSequence.collect)

    val resultSGPIntersection = SGP.intersection(SGP2)

    val expectedVerticesIntersection: RDD[(VertexId, (Interval, Set[String]))] = ProgramContext.sc.parallelize(Array(
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), Set("c", "C"))),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set("d", "d1"))),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), Set("e", "E")))
    ))

    val expectedEdgesIntersection: RDD[((VertexId, VertexId), (Interval, Set[Int]))] = ProgramContext.sc.parallelize(Array(
      ((3L, 3L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), Set(42, 22))),
      ((4L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set(42, 52)))
    ))
    val expectedSGPIntersection = SnapshotGraphParallel.fromRDDs(expectedVerticesIntersection, expectedEdgesIntersection, Set("Default"), StorageLevel.MEMORY_ONLY_SER)

    assert(resultSGPIntersection.vertices.collect.toSet === expectedVerticesIntersection.collect.toSet)
    assert(resultSGPIntersection.edges.collect.toSet === expectedEdgesIntersection.collect.toSet)
    assert(resultSGPIntersection.getTemporalSequence.collect === expectedSGPIntersection.getTemporalSequence.collect)

  }

  test("Union and intersection - when there is no overlap between two graphs") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b"))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42))
    ))

    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))

    val edges2: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((2L, 3L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52))
    ))

    val SGP2 = SnapshotGraphParallel.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, Set[String]))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set("a"))),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set("b"))),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), Set("b1"))),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), Set("C")))
    ))

    val expectedEdgesUnion: RDD[((VertexId, VertexId), (Interval, Set[Int]))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set(42))),
      ((2L, 3L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), Set(52)))
    ))

    val expectedSGPUnion = SnapshotGraphParallel.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, Set("Default"), StorageLevel.MEMORY_ONLY_SER)

    val resultSGPUnion = SGP.union(SGP2)

    assert(resultSGPUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultSGPUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultSGPUnion.getTemporalSequence.collect === expectedSGPUnion.getTemporalSequence.collect)

    val resultSGPIntersection = SGP.intersection(SGP2)

    assert(resultSGPIntersection.vertices.collect.toSet === SnapshotGraphParallel.emptyGraph().vertices.collect.toSet)
    assert(resultSGPIntersection.edges.collect.toSet === SnapshotGraphParallel.emptyGraph().edges.collect.toSet)
    assert(resultSGPIntersection.getTemporalSequence.collect === Seq[Interval]())
  }

  test("Union and intersection -when graph.span.start == graph2.span.end") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42))
    ))
    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))
    val edges2: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((2L, 3L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52))
    ))
    val SGP2 = SnapshotGraphParallel.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, Set[String]))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), Set("a"))),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set("b"))),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Set("b1"))),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), Set("C")))
    ))
    val expectedEdgesUnion: RDD[((VertexId, VertexId), (Interval, Set[Int]))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set(42))),
      ((2L, 3L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), Set(52)))
    ))

    val expectedSGPUnion = SnapshotGraphParallel.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, Set("Default"), StorageLevel.MEMORY_ONLY_SER)

    val resultSGPUnion = SGP.union(SGP2)

    assert(resultSGPUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultSGPUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultSGPUnion.getTemporalSequence.collect === expectedSGPUnion.getTemporalSequence.collect)

    val resultSGPIntersection = SGP.intersection(SGP2)

    assert(resultSGPIntersection.vertices.collect.toSet === SnapshotGraphParallel.emptyGraph().vertices.collect.toSet)
    assert(resultSGPIntersection.edges.collect.toSet === SnapshotGraphParallel.emptyGraph().edges.collect.toSet)
    assert(resultSGPIntersection.getTemporalSequence.collect === Seq[Interval]())
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
    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val actualSGP = SGP.project(edge => (edge.attr * edge.attr), (vertex, name) => name.toUpperCase, "Default")

    val expectedVertices: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2016-01-01")), "B")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "C")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "D"))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 16)),
      ((1L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 4))
    ))
    val expectedSGP = SnapshotGraphParallel.fromRDDs(expectedVertices, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedVertices.collect().toSet === actualSGP.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualSGP.edges.collect().toSet)
    assert(actualSGP.getTemporalSequence.collect === expectedSGP.getTemporalSequence.collect)
  }

  test("verticesAggregated and edgesAggregated functions") {
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
      (1L, new Object2ObjectOpenHashMap(Array(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2022-01-01"))), Array("John"))),
      (2L, new Object2ObjectOpenHashMap(Array(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), Interval(LocalDate.parse("2018-02-01"), LocalDate.parse("2020-01-01"))), Array("Mike",  "Mike"))),
      (3L, new Object2ObjectOpenHashMap(Array(Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), Interval(LocalDate.parse("2019-01-01"), LocalDate.parse("2022-01-01"))), Array("Ron",  "Ron"))),
      (4L, new Object2ObjectOpenHashMap(Array(Interval(LocalDate.parse("2006-01-01"), LocalDate.parse("2017-01-01")), Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2019-07-14"))), Array("Julia",  "Vera")))
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

    val expectedEdges: RDD[((VertexId, VertexId), Map[Interval, Int])] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), new Object2IntOpenHashMap(Array(Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01"))), Array(42, 12, 56)).asInstanceOf[Map[Interval,Int]]),
      ((3L, 5L), new Object2IntOpenHashMap(Array(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01"))), Array(42, 42)).asInstanceOf[Map[Interval,Int]]),
      ((1L, 2L), new Object2IntOpenHashMap(Array(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01"))), Array(22)).asInstanceOf[Map[Interval,Int]]),
      ((5L, 7L), new Object2IntOpenHashMap(Array(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01"))), Array(22)).asInstanceOf[Map[Interval,Int]]),
      ((4L, 8L), new Object2IntOpenHashMap(Array(Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01"))), Array(42)).asInstanceOf[Map[Interval,Int]])
    ))

    val actualSGP = SnapshotGraphParallel.fromRDDs(vertices, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(actualSGP.verticesAggregated.collect.toSet === expectedVertices.collect.toSet)
    assert(actualSGP.edgesAggregated.collect.toSet === expectedEdges.collect.toSet)
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
    val SGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    //should be empty
    val graph1 = SGP.getSnapshot(LocalDate.parse("2008-12-01"))
    val graph2 = SGP.getSnapshot(LocalDate.parse("2018-01-01"))

    assert(graph1.vertices.isEmpty())
    assert(graph1.edges.isEmpty())

    assert(graph2.vertices.isEmpty())
    assert(graph2.edges.isEmpty())

    //not empty
    val graph3 = SGP.getSnapshot(LocalDate.parse("2009-01-01"))
    val expectedUsers3 = ProgramContext.sc.parallelize(Array(
      (3L, "Ron")
    ))
    assert(expectedUsers3.collect.toSet === graph3.vertices.collect.toSet)
    assert(graph3.edges.isEmpty())

    val graph4 = SGP.getSnapshot(LocalDate.parse("2010-01-01"))
    val expectedUsers4 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (3L, "Ron")
    ))
    val expectedEdges4 = ProgramContext.sc.parallelize(Array(
      Edge(1L, 3L, 42)
    ))
    assert(expectedUsers4.collect.toSet === graph4.vertices.collect.toSet)
    assert(expectedEdges4.collect.toSet === graph4.edges.collect.toSet)

    val graph5 = SGP.getSnapshot(LocalDate.parse("2012-01-01"))
    val expectedUsers5 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (3L, "Ron")
    ))
    assert(expectedUsers5.collect.toSet === graph5.vertices.collect.toSet)
    assert(graph5.edges.isEmpty())

    val graph6 = SGP.getSnapshot(LocalDate.parse("2014-01-01"))
    val expectedUsers6 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (2L, "Mike")
    ))
    val expectedEdges6 = ProgramContext.sc.parallelize(Array(
      Edge(1L, 2L, 22)
    ))
    assert(expectedUsers6.collect.toSet === graph6.vertices.collect.toSet)
    assert(expectedUsers6.collect.toSet === graph6.vertices.collect.toSet)

    val graph7 = SGP.getSnapshot(LocalDate.parse("2016-01-01"))
    val expectedUsers7 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (2L, "Mike")
    ))
    assert(expectedUsers7.collect.toSet === graph7.vertices.collect.toSet)
    assert(graph7.edges.isEmpty())

    val graph8 = SGP.getSnapshot(LocalDate.parse("2017-01-01"))
    val expectedUsers8 = ProgramContext.sc.parallelize(Array(
      (2L, "Mike")
    ))
    assert(expectedUsers8.collect.toSet === graph8.vertices.collect.toSet)
    assert(graph8.edges.isEmpty())
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
    var edges2: EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.parallelize(Array(
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
    var edges3: EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.parallelize(Array(
      Edge(7L, 8L, 22)
    )))
    val graph3 = Graph(users3, edges3, "Default", edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)

    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-01-01"))

    intervals = intervals :+ testInterval1 :+ testInterval2 :+ testInterval3
    graphs = graphs :+ graph1 :+ graph2 :+ graph3

    val actualSgp = SnapshotGraphParallel.fromGraphs(ProgramContext.sc.parallelize(intervals), graphs, "Default", StorageLevel.MEMORY_ONLY_SER)

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
    val expectedSGP = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(users.collect().toSet === actualSgp.vertices.collect().toSet)
    assert(edges.collect().toSet === actualSgp.edges.collect().toSet)
    assert(actualSgp.getTemporalSequence.collect === expectedSGP.getTemporalSequence.collect)
  }


  test("connected components") {
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
      ((2L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), 42)),
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
      ((4L, 8L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((6L, 8L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42))
    ))

    val expectedNodes: RDD[(VertexId, (Interval, (String, Int)))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("John", 1))),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Mike", 1))),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Mike", 2))),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Ron", 1))),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Julia", 1))),
      (4L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Julia", 2))),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Vera", 1))),
      (6L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Halima", 1))),
      (6L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Halima", 2))),
      (7L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Sanjana", 7))),
      (7L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Sanjana", 1))),
      (8L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Lovro", 7))),
      (8L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Lovro", 2)))
    ))

    val SGP = SnapshotGraphParallel.fromRDDs(nodes, edges, "Default")

    val actualSGP = SGP.connectedComponents()
    assert(actualSGP.vertices.collect.toSet == expectedNodes.collect.toSet)
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
      ((2L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((3L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((4L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((5L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((7L, 8L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),

      //second representative graph
      ((1L, 3L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((1L, 5L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((3L, 7L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((5L, 7L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42))
      //I dont have the last two edges used in other tests because I want to test coalescing.
      // Here, 8 is coalesced.
    ))

    val expectedNodes: RDD[(VertexId, (Interval, (String, Map[VertexId, Int])))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("John", new Long2IntOpenHashMap(Array(1L, 2L), Array(0, 1)).asInstanceOf[Map[VertexId,Int]]))),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Mike", new Long2IntOpenHashMap(Array(1L, 2L), Array(1, 0)).asInstanceOf[Map[VertexId,Int]]))),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Ron", new Long2IntOpenHashMap(Array(1L, 2L), Array(2, 1)).asInstanceOf[Map[VertexId,Int]]))),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Julia", new Long2IntOpenHashMap(Array(1L, 2L), Array(2, 1)).asInstanceOf[Map[VertexId,Int]]))),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Vera", new Long2IntOpenHashMap(Array(1L, 2L), Array(3, 2)).asInstanceOf[Map[VertexId,Int]]))),
      (6L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Halima", new Long2IntOpenHashMap(Array(1L, 2L), Array(2, 1)).asInstanceOf[Map[VertexId,Int]]))),
      (7L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Sanjana", new Long2IntOpenHashMap().asInstanceOf[Map[VertexId, Int]]))),
      (8L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Lovro", new Long2IntOpenHashMap().asInstanceOf[Map[VertexId, Int]]))),

      //second representative graph
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("John", new Long2IntOpenHashMap(Array(1L), Array(0)).asInstanceOf[Map[VertexId,Int]]))),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Mike", new Long2IntOpenHashMap(Array(2L), Array(0)).asInstanceOf[Map[VertexId,Int]]))),
      (3L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Ron", new Long2IntOpenHashMap(Array(1L), Array(1)).asInstanceOf[Map[VertexId,Int]]))),
      (4L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Julia", new Long2IntOpenHashMap(Array(2L), Array(1)).asInstanceOf[Map[VertexId,Int]]))),
      (5L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Vera", new Long2IntOpenHashMap(Array(1L), Array(1)).asInstanceOf[Map[VertexId,Int]]))),
      (6L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Halima", new Long2IntOpenHashMap(Array(2L), Array(1)).asInstanceOf[Map[VertexId,Int]]))),
      (7L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Sanjana", new Long2IntOpenHashMap(Array(1L), Array(2)).asInstanceOf[Map[VertexId,Int]])))
    ))

    val SGP = SnapshotGraphParallel.fromRDDs(nodes, edges, "Default")

    val actualSGP = SGP.shortestPaths(false, Seq(1L, 2L))
    assert(actualSGP.vertices.collect.toSet == expectedNodes.collect.toSet)
  }

  test("directed shortestPath") {
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
      ((2L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((3L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((4L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((5L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((7L, 8L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),

      //second representative graph
      ((1L, 3L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((1L, 5L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((3L, 7L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((5L, 7L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42))
      //I dont have the last two edges used in other tests because I want to test coalescing.
      // Here, 8 is coalesced.
    ))

    val expectedNodes: RDD[(VertexId, (Interval, (String, Map[VertexId, Int])))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("John", scala.collection.Map(5L -> 3, 6L -> 2).asJava))),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Mike", scala.collection.Map(5L -> 2, 6L -> 1).asJava))),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Ron", scala.collection.Map(5L -> 1, 6L -> 2).asJava))),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Julia", scala.collection.Map(5L -> 1, 6L -> 2).asJava))),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Vera", scala.collection.Map(5L -> 0, 6L -> 1).asJava))),
      (6L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Halima", scala.collection.Map(6L ->0).asJava))),
      (7L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Sanjana", scala.collection.Map[VertexId,Int]().asJava))),
      (8L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Lovro", scala.collection.Map[VertexId,Int]().asJava))),

      //second representative graph
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("John", scala.collection.Map(5L -> 1).asJava))),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Mike", scala.collection.Map(6L -> 1).asJava))),
      (3L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Ron", scala.collection.Map[VertexId,Int]().asJava))),
      (4L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Julia", scala.collection.Map[VertexId,Int]().asJava))),
      (5L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Vera", scala.collection.Map(5L -> 0).asJava)))
    ))

    val SGP = SnapshotGraphParallel.fromRDDs(nodes, edges, "Default")

    val actualSGP = SGP.shortestPaths(true, Seq(5L, 6L))
    assert(actualSGP.vertices.collect.toSet === expectedNodes.collect.toSet)
  }

  test("directed pagerank") {
    //PageRank for each representative graph was tested by creating graph in graphX and using spark's pagerank
    //The final SGP is sliced into the two representative graph to assert the values
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
      ((2L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), 42)),
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
      ((4L, 8L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((6L, 8L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42))
    ))

    //Pagerank using spark's api
    val testNodes: RDD[(VertexId, String)] = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (2L, "Mike"),
      (3L, "Ron"),
      (4L, "Julia"),
      (5L, "Vera"),
      (6L, "Halima"),
      (7L, "Sanjana"),
      (8L, "Lovro")
    ))
    val testEdges: RDD[Edge[Int]] = ProgramContext.sc.parallelize(Array(
      Edge(1L, 2L, 42),
      Edge(2L, 3L, 42),
      Edge(2L, 6L, 42),
      Edge(2L, 4L, 42),
      Edge(3L, 5L, 42),
      Edge(3L, 4L, 42),
      Edge(4L, 5L, 42),
      Edge(5L, 6L, 42),
      Edge(7L, 8L, 42)
    ))
    val graph1 = Graph(testNodes, testEdges, "Default")
    val pageRank2010_2014 = graph1.staticPageRank(10, 0.15)

    val testNodes2: RDD[(VertexId, String)] = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (2L, "Mike"),
      (3L, "Ron"),
      (4L, "Julia"),
      (5L, "Vera"),
      (6L, "Halima"),
      (7L, "Sanjana"),
      (8L, "Lovro")
    ))
    val testEdges2: RDD[Edge[Int]] = ProgramContext.sc.parallelize(Array(
      Edge(1L, 3L, 42),
      Edge(1L, 5L, 42),
      Edge(3L, 7L, 42),
      Edge(5L, 7L, 42),
      Edge(2L, 4L, 42),
      Edge(2L, 6L, 42),
      Edge(4L, 8L, 42),
      Edge(6L, 8L, 42)
    ))
    val graph2 = Graph(testNodes2, testEdges2, "Default")
    val pageRank2014_2018 = graph2.staticPageRank(10, 0.15)

    val pageRank2010_2014VerticesSorted = pageRank2010_2014.vertices.sortBy(_._1).collect()
    val pageRank2014_2018VerticesSorted = pageRank2014_2018.vertices.sortBy(_._1).collect()
    //End of Spark's pagerank

    //Pagerank using Portal api
    val SGP = SnapshotGraphParallel.fromRDDs(nodes, edges, "Default")
    val actualSGP = SGP.pageRank(true, 0.001, 0.15, 10)
    val sliced2010_2014 = actualSGP.slice(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")))
    val sliced2014_2018 = actualSGP.slice(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")))
    val actualSGP2010_2014VerticesSorted = sliced2010_2014.vertices.sortBy(_._1).collect()
    val actualSGP2014_2018VerticesSorted = sliced2014_2018.vertices.sortBy(_._1).collect()
    //End of Portal pagerank


    //Assertion
    for (i <- 0 until pageRank2010_2014VerticesSorted.length) {
      val difference = pageRank2010_2014VerticesSorted(i)._2 - actualSGP2010_2014VerticesSorted(i)._2._2._2
      assert(Math.abs(difference) < 0.0000001)
    }

    for (i <- 0 until pageRank2014_2018VerticesSorted.length) {
      val difference = pageRank2014_2018VerticesSorted(i)._2 - actualSGP2014_2018VerticesSorted(i)._2._2._2
      assert(Math.abs(difference) < 0.0000001)
    }
  }

  test("undirected pagerank") {
    //PageRank for each representative graph was tested by creating graph in graphX and using spark's pagerank
    //Spark's pagerank only has directed pagerank so to test it, each edge is added both ways and spark's pagerank is computed
    //The final SGP is sliced into the two representative graph to assert the values
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
      ((2L, 6L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((2L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), 42)),
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
      ((4L, 8L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)),
      ((6L, 8L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42))
    ))

    //Pagerank using spark's api
    val testNodes: RDD[(VertexId, String)] = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (2L, "Mike"),
      (3L, "Ron"),
      (4L, "Julia"),
      (5L, "Vera"),
      (6L, "Halima"),
      (7L, "Sanjana"),
      (8L, "Lovro")
    ))
    val testEdges: RDD[Edge[Int]] = ProgramContext.sc.parallelize(Array(
      Edge(1L, 2L, 42),
      Edge(2L, 3L, 42),
      Edge(2L, 6L, 42),
      Edge(2L, 4L, 42),
      Edge(3L, 5L, 42),
      Edge(3L, 4L, 42),
      Edge(4L, 5L, 42),
      Edge(5L, 6L, 42),
      Edge(7L, 8L, 42),

      Edge(2L, 1L, 42),
      Edge(3L, 2L, 42),
      Edge(6L, 2L, 42),
      Edge(4L, 2L, 42),
      Edge(5L, 3L, 42),
      Edge(4L, 3L, 42),
      Edge(5L, 4L, 42),
      Edge(6L, 5L, 42),
      Edge(8L, 7L, 42)
    ))
    val graph1 = Graph(testNodes, testEdges, "Default")
    val pageRank2010_2014 = graph1.staticPageRank(10, 0.15)

    val testNodes2: RDD[(VertexId, String)] = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (2L, "Mike"),
      (3L, "Ron"),
      (4L, "Julia"),
      (5L, "Vera"),
      (6L, "Halima"),
      (7L, "Sanjana"),
      (8L, "Lovro")
    ))
    val testEdges2: RDD[Edge[Int]] = ProgramContext.sc.parallelize(Array(
      Edge(1L, 3L, 42),
      Edge(1L, 5L, 42),
      Edge(3L, 7L, 42),
      Edge(5L, 7L, 42),
      Edge(2L, 4L, 42),
      Edge(2L, 6L, 42),
      Edge(4L, 8L, 42),
      Edge(6L, 8L, 42),

      Edge(3L, 1L, 42),
      Edge(5L, 1L, 42),
      Edge(7L, 3L, 42),
      Edge(7L, 5L, 42),
      Edge(4L, 2L, 42),
      Edge(6L, 2L, 42),
      Edge(8L, 4L, 42),
      Edge(8L, 6L, 42)
    ))
    val graph2 = Graph(testNodes2, testEdges2, "Default")
    val pageRank2014_2018 = graph2.staticPageRank(10, 0.15)

    val pageRank2010_2014VerticesSorted = pageRank2010_2014.vertices.sortBy(_._1).collect()
    val pageRank2014_2018VerticesSorted = pageRank2014_2018.vertices.sortBy(_._1).collect()
    //End of Spark's pagerank

    //Pagerank using Portal api
    val SGP = SnapshotGraphParallel.fromRDDs(nodes, edges, "Default")
    val actualSGP = SGP.pageRank(false, 0.001, 0.15, 10)
    val sliced2010_2014 = actualSGP.slice(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")))
    val sliced2014_2018 = actualSGP.slice(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")))
    val actualSGP2010_2014VerticesSorted = sliced2010_2014.vertices.sortBy(_._1).collect()
    val actualSGP2014_2018VerticesSorted = sliced2014_2018.vertices.sortBy(_._1).collect()
    //End of Portal pagerank


    //Assertion
    for (i <- 0 until pageRank2010_2014VerticesSorted.length) {
      val difference = pageRank2010_2014VerticesSorted(i)._2 - actualSGP2010_2014VerticesSorted(i)._2._2._2
      assert(Math.abs(difference) < 0.0000001)
    }

    for (i <- 0 until pageRank2014_2018VerticesSorted.length) {
      val difference = pageRank2014_2018VerticesSorted(i)._2 - actualSGP2014_2018VerticesSorted(i)._2._2._2
      assert(Math.abs(difference) < 0.0000001)
    }
  }

}
