package edu.drexel.cs.dbgroup.temporalgraph.representations

import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph.{Interval, ProgramContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Created by amir on 12/15/16.
  */
class VEGraphSuite  extends FunSuite with BeforeAndAfter {

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


  test("Union, Intersection and Difference") {
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

    val VEG = VEGraph.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

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

    val VEG2 = VEGraph.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

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

    val resultOGCUnion = VEG.union(VEG2)
    val expectedOGCUnion = VEGraph.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, Set("Default"), StorageLevel.MEMORY_ONLY_SER)

    assert(resultOGCUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultOGCUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultOGCUnion.getTemporalSequence.collect === expectedOGCUnion.getTemporalSequence.collect)

    val resultOGCIntersection = VEG.intersection(VEG2)

    val expectedVerticesIntersection: RDD[(VertexId, (Interval, Set[String]))] = ProgramContext.sc.parallelize(Array(
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), Set("c", "C"))),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set("d", "d1"))),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), Set("e", "E")))
    ))

    val expectedEdgesIntersection: RDD[((VertexId, VertexId), (Interval, Set[Int]))] = ProgramContext.sc.parallelize(Array(
      ((3L, 3L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), Set(42, 22))),
      ((4L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), Set(42, 52)))
    ))
    val expectedOGCIntersection = VEGraph.fromRDDs(expectedVerticesIntersection, expectedEdgesIntersection, Set("Default"), StorageLevel.MEMORY_ONLY_SER)

    assert(resultOGCIntersection.vertices.collect.toSet === expectedVerticesIntersection.collect.toSet)
    assert(resultOGCIntersection.edges.collect.toSet === expectedEdgesIntersection.collect.toSet)
    assert(resultOGCIntersection.getTemporalSequence.collect === expectedOGCIntersection.getTemporalSequence.collect)


    val result‌VEGDifference = VEG.difference(VEG2)

    val expectedVerticesDifference: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), "c")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), "e")),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), "e"))

    ))

    val expectedEdgesDifference: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((3L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((2L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 42)),
      ((2L, 5L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), 42))

    ))
    val expected‌VEGDifference = VEGraph.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(result‌VEGDifference.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(result‌VEGDifference.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(result‌VEGDifference.getTemporalSequence.collect === expected‌VEGDifference.getTemporalSequence.collect)

  }


  test("Difference 2") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), "b")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), "c")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), "d")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), "e"))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), 42)),
      ((2L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), 42)),
      ((3L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), 42)),
      ((4L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), 42))
    ))

    val VEG = VEGraph.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

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

    val VEG2 = VEGraph.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    val result‌VEGIntersection = VEG.difference(VEG2)

    val expectedVerticesDifference: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
        (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "a")),
        (1L, (Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), "a")),
        (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b")),
        (2L, (Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), "b")),
        (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), "c")),
        (3L, (Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), "c")),
        (4L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2019-01-01")), "d")),
        (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), "e")),
        (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2019-01-01")), "e"))
        ))

    val expectedEdgesDifference: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), 42)),
      ((2L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((2L, 3L), (Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), 42)),
      ((3L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((3L, 3L), (Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), 42)),
      ((4L, 4L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2019-01-01")), 42))
    ))
    val expected‌VEGDifference = VEGraph.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(result‌VEGIntersection.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(result‌VEGIntersection.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(result‌VEGIntersection.getTemporalSequence.collect === expected‌VEGDifference.getTemporalSequence.collect)

  }


  test("Union, intersection and Difference - when there is no overlap between two graphs") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b"))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42))
    ))

    val VEG = VEGraph.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))

    val edges2: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((2L, 3L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52))
    ))

    val VEG2 = VEGraph.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

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

    val expectedVECUnion = VEGraph.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, Set("Default"), StorageLevel.MEMORY_ONLY_SER)

    val resultVEGCUnion = VEG.union(VEG2)

    assert(resultVEGCUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultVEGCUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultVEGCUnion.getTemporalSequence.collect === expectedVECUnion.getTemporalSequence.collect)

    val resultVEGIntersection = VEG.intersection(VEG2)

    assert(resultVEGIntersection.vertices.collect.toSet === OneGraphColumn.emptyGraph("").vertices.collect.toSet)
    assert(resultVEGIntersection.edges.collect.toSet === OneGraphColumn.emptyGraph("").edges.collect.toSet)
    assert(resultVEGIntersection.getTemporalSequence.collect === Seq[Interval]())

    val resultVEGDifference = VEG.difference(VEG2)

    val expectedVerticesDifference: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b"))

    ))

    val expectedEdgesDifference: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42))
    ))
    val expectedVEGDifference = VEGraph.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(resultVEGDifference.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(resultVEGDifference.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(resultVEGDifference.getTemporalSequence.collect === expectedVEGDifference.getTemporalSequence.collect)
  }

  test("Union, intersection and Difference -when graph.span.start == graph2.span.end") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42))
    ))
    val VEG = VEGraph.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))
    val edges2: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((2L, 3L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52))
    ))
    val VEG2 = VEGraph.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

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
    val expectedOGCUnion = VEGraph.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, Set("Default"), StorageLevel.MEMORY_ONLY_SER)

    val resultOGCUnion = VEG.union(VEG2)

    assert(resultOGCUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultOGCUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultOGCUnion.getTemporalSequence.collect === expectedOGCUnion.getTemporalSequence.collect)

    val resultOGCIntersection = VEG.intersection(VEG2)

    assert(resultOGCIntersection.vertices.collect.toSet === OneGraphColumn.emptyGraph("").vertices.collect.toSet)
    assert(resultOGCIntersection.edges.collect.toSet === OneGraphColumn.emptyGraph("").edges.collect.toSet)
    assert(resultOGCIntersection.getTemporalSequence.collect === Seq[Interval]())

    val resultVEGDifference = VEG.difference(VEG2)

    val expectedVerticesDifference: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b"))

    ))

    val expectedEdgesDifference: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42))
    ))
    val expectedVEGDifference = VEGraph.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(resultVEGDifference.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(resultVEGDifference.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(resultVEGDifference.getTemporalSequence.collect === expectedVEGDifference.getTemporalSequence.collect)
  }


}
