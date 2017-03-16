package edu.drexel.cs.dbgroup.temporalgraph.representations

import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph.{Always, ChangeSpec, Exists, Resolution, TimeSpec, _}
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

    val expectedVerticesUnion: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "A")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), "c")),
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "c"+"C")),
      (3L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "C")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d"+"d1")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), "e")),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), "e"+"E")),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), "e"))
    ))

    val expectedEdgesUnion: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 22)),
      ((2L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 3L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 52)),
      ((3L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((3L, 3L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 22)),
      ((4L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 52)),
      ((2L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((5L, 5L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), 22))
    ))


    val resultOGCUnion = VEG.union(VEG2, (x,y)=>x+y , (x,y)=>math.max(x,y))
    val expectedOGCUnion = VEGraph.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(resultOGCUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultOGCUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultOGCUnion.getTemporalSequence.collect === expectedOGCUnion.getTemporalSequence.collect)

    val resultOGCIntersection = VEG.intersection(VEG2, (x,y)=>x+y , (x,y)=>math.max(x,y))

    val expectedVerticesIntersection: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "c"+"C")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d"+"d1")),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), "e"+"E"))
    ))


    val expectedEdgesIntersection: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((3L, 3L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((4L, 4L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 52))
    ))
    val expectedOGCIntersection = VEGraph.fromRDDs(expectedVerticesIntersection, expectedEdgesIntersection, "Default", StorageLevel.MEMORY_ONLY_SER)

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

    val expectedVerticesUnion: RDD[(VertexId, (Interval,String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))

    val expectedEdgesUnion: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 3L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52))
    ))

    val expectedVECUnion = VEGraph.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resultVEGCUnion = VEG.union(VEG2, (x,y)=>x+y , (x,y)=>math.max(x,y))

    assert(resultVEGCUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultVEGCUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultVEGCUnion.getTemporalSequence.collect === expectedVECUnion.getTemporalSequence.collect)

    val resultVEGIntersection = VEG.intersection(VEG2, (x,y)=>x+y , (x,y)=>math.max(x,y))

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

    val expectedVerticesUnion: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))
    val expectedEdgesUnion: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 2L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)),
      ((2L, 3L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52))
    ))
    val expectedOGCUnion = VEGraph.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resultOGCUnion = VEG.union(VEG2, (x,y)=>x+y , (x,y)=>math.max(x,y))

    assert(resultOGCUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultOGCUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultOGCUnion.getTemporalSequence.collect === expectedOGCUnion.getTemporalSequence.collect)

    val resultOGCIntersection = VEG.intersection(VEG2, (x,y)=>x+y , (x,y)=>math.max(x,y))

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

  test("aggregateMessages - no predicate") {

    val nodesAndEdges = AggregateMessagesTestUtil.getNodesAndEdges_v1

    var g = VEGraph.fromRDDs(nodesAndEdges._1,nodesAndEdges._2,"Default")

    val result = g.aggregateMessages[Int](AggregateMessagesTestUtil.sendMsg_noPredicate, (a, b) => {a+b}, 0, TripletFields.None)
      .asInstanceOf[VEGraph[(String,Int),Int]]

    AggregateMessagesTestUtil.assertions_noPredicate(result)
  }

  test("aggregateMessages - edge predicate") {

    val nodesAndEdges = AggregateMessagesTestUtil.getNodesAndEdges_v1

    var g = VEGraph.fromRDDs(nodesAndEdges._1,nodesAndEdges._2,"Default")

    val result = g.aggregateMessages[Int](AggregateMessagesTestUtil.sendMsg_edgePredicate, (a, b) => {a+b}, 0, TripletFields.EdgeOnly)
      .asInstanceOf[VEGraph[(String,Int),Int]]

    AggregateMessagesTestUtil.assertions_edgePredicate(result)
  }

  test("aggregateMessages - vertex predicate") {

    val nodesAndEdges = AggregateMessagesTestUtil.getNodesAndEdges_v1

    var g = VEGraph.fromRDDs(nodesAndEdges._1,nodesAndEdges._2,"Default")

    val result = g.aggregateMessages[Int](AggregateMessagesTestUtil.sendMsg_vertexPredicate, (a, b) => {a+b}, 0, TripletFields.All)
      .asInstanceOf[VEGraph[(String,Int),Int]]

    AggregateMessagesTestUtil.assertions_vertexPredicate(result)
  }

  test("createTemporalNodes aggregateByTime -with structure only") {
    val users: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), true)),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2018-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), true)),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), true)),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), true)),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), true))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), true)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), true)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), true)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), true)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), true)),
      ((4L, 6L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), true)),
      ((4L, 6L), (Interval(LocalDate.parse("2012-06-01"), LocalDate.parse("2013-01-01")), true))

    ))
    val VEG = VEGraph.fromRDDs(users, edges, true, StorageLevel.MEMORY_ONLY_SER)

    val resolution1Month = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2011-02-01"))
    val resolution3Years = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2014-01-01"))

    val actualVEG = VEG.createTemporalNodes(new TimeSpec(resolution3Years), Always(), Always(), (attr1, attr2) => attr2, (attr1, attr2) => attr2)

    val expectedVertices: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2012-01-01")), true)),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2018-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), true))
    ))

    val expectedEdges: RDD[((VertexId, VertexId), (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true))
    ))
    val expectedVEG = VEGraph.fromRDDs(expectedVertices, expectedEdges, true, StorageLevel.MEMORY_ONLY_SER)

    assert(expectedVertices.collect().toSet === actualVEG.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualVEG.edges.collect().toSet)
    assert(expectedVEG.getTemporalSequence.collect === actualVEG.getTemporalSequence.collect)

    val actualVEG2 = VEG.createTemporalNodes(new TimeSpec(resolution3Years), Always(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) =>  attr2)

    val expectedVertices2: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2012-01-01")), true)),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2018-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), true))
    ))

    val expectedEdges2: RDD[((VertexId, VertexId), (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      ((4L, 6L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true))
    ))
    val expectedVEG2 = VEGraph.fromRDDs(expectedVertices2, expectedEdges2, true, StorageLevel.MEMORY_ONLY_SER)

    assert(expectedVertices2.collect().toSet === actualVEG2.vertices.collect().toSet)
    assert(expectedEdges2.collect().toSet === actualVEG2.edges.collect().toSet)
    assert(expectedVEG2.getTemporalSequence.collect === actualVEG2.getTemporalSequence.collect)
  }

  test("createTemporalNodes aggregateByChange -w/o structural") {
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

    val VEG = VEGraph.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)
    val actualVEG = VEG.createTemporalNodes(new ChangeSpec(1), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)
    val expectedVEG = VEGraph.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(users.collect().toSet === actualVEG.vertices.collect().toSet)
    assert(edges.collect().toSet === actualVEG.edges.collect().toSet)
    assert(expectedVEG.getTemporalSequence.collect === actualVEG.getTemporalSequence.collect)
    val actualVEG2 = VEG.createTemporalNodes(new ChangeSpec(2), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)
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
    val expectedVEG2 = VEGraph.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedUsers.collect().toSet === actualVEG2.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualVEG2.edges.collect().toSet)
    assert(expectedVEG2.getTemporalSequence.collect === actualVEG2.getTemporalSequence.collect)

    val actualVEG3 = VEG.createTemporalNodes(new ChangeSpec(2), Always(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)

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
    val expectedVEG3 = VEGraph.fromRDDs(expectedUsers3, expectedEdges3, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedUsers3.collect().toSet === actualVEG3.vertices.collect().toSet)
    assert(expectedEdges3.collect().toSet === actualVEG3.edges.collect().toSet)
    assert(expectedVEG3.getTemporalSequence.collect === actualVEG3.getTemporalSequence.collect)

  }


  test("createTemporalNodes aggregateByChange -with structural only") {
    val users: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), true)),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), true)),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), true)),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), true)),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), true))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), true)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), true)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), true)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), true)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), true)),
      ((4L, 6L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true))
    ))

    val VEG = VEGraph.fromRDDs(users, edges, true, StorageLevel.MEMORY_ONLY_SER)
    val actualVEG = VEG.createTemporalNodes(new ChangeSpec(1), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)
    val expectedVEG = VEGraph.fromRDDs(users, edges, true, StorageLevel.MEMORY_ONLY_SER)

    assert(users.collect().toSet === actualVEG.vertices.collect().toSet)
    assert(edges.collect().toSet === actualVEG.edges.collect().toSet)
    assert(expectedVEG.getTemporalSequence.collect === actualVEG.getTemporalSequence.collect)

    val actualVEG2 = VEG.createTemporalNodes(new ChangeSpec(2), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)
    val expectedUsers: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2017-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), true)),
      (4L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2017-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), true)),
      (6L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), true)),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), true)),
      (8L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), true)),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), true))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), true)),
      ((3L, 5L), (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2013-01-01")), true)),
      ((1L, 2L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2017-01-01")), true)),
      ((5L, 7L), (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), true)),
      ((4L, 8L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), true)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), true)),
      ((4L, 6L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), true))
    ))
    val expectedVEG2 = VEGraph.fromRDDs(expectedUsers, expectedEdges, true, StorageLevel.MEMORY_ONLY_SER)

    assert(expectedUsers.collect().toSet === actualVEG2.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualVEG2.edges.collect().toSet)
    assert(expectedVEG2.getTemporalSequence.collect === actualVEG2.getTemporalSequence.collect)

    val actualVEG3 = VEG.createTemporalNodes(new ChangeSpec(2), Always(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)

    val expectedUsers3: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2017-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2013-01-01")), true)),
      (4L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2017-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), true)),
      (6L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), true)),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), true))
    ))
    val expectedEdges3: RDD[((VertexId, VertexId), (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), true)),
      ((3L, 5L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2013-01-01")), true)),
      ((1L, 2L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), true)),
      ((4L, 6L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), true))
    ))
    val expectedVEG3 = VEGraph.fromRDDs(expectedUsers3, expectedEdges3, true, StorageLevel.MEMORY_ONLY_SER)

    assert(expectedUsers3.collect().toSet === actualVEG3.vertices.collect().toSet)
    assert(expectedUsers3.collect().toSet === actualVEG3.vertices.collect().toSet)
    assert(expectedEdges3.collect().toSet === actualVEG3.edges.collect().toSet)
    assert(expectedVEG3.getTemporalSequence.collect === actualVEG3.getTemporalSequence.collect)
  }

  test("createAttributeNodes") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2012-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2013-01-01")), "ab")),
      (3L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), "abc")),
      (4L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), "abcd")),
      (5L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), "abcde")),
      (6L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), "abcdef")),
      (7L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2018-01-01")), "abcdefg"))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L,2L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), 40)),
      ((2L,3L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), 50)),
      ((3L,4L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 60)),
      ((4L,5L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), 70)),
      ((5L,6L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01")), 80)),
      ((6L,7L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 90))
    ))

    val VEG = VEGraph.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val longerString = (a: String, b: String) =>
      if (a.length > b.length) a else if (a.length < b.length) b else if (a.compareTo(b) > 0) a else b

    val actualVEG = VEG.createAttributeNodes( (name1, name2) => longerString(name1, name2), (attr1, attr2) => Math.max(attr1, attr2))((vid, attr1) => if (attr1.length < 5) 1L else 2L)

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), "a")),
      (1L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), "ab")),
      (1L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), "abc")),
      (1L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), "abcd")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), "abcde")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01")), "abcdef")),
      (2L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2018-01-01")), "abcdefg"))
    ))

    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L,1L), (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), 40)),
      ((1L,1L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), 50)),
      ((1L,1L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 60)),
      ((1L,2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), 70)),
      ((2L,2L), (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01")), 80)),
      ((2L,2L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 90))
    ))
    val expectedVEG = VEGraph.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedUsers.collect().toSet === actualVEG.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualVEG.edges.collect().toSet)
    assert(expectedVEG.getTemporalSequence.collect === actualVEG.getTemporalSequence.collect)

  }



}
