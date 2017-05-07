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
class VEGraphSuite  extends RepresentationsTestSuite {
  lazy val empty = VEGraph.emptyGraph[String,Int]("Default")

  test("Union, Intersection and Difference") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "c")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "e"))
    ))

    val edges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](3L, 3L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](5L, 2L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)
    ))

    val VEG = VEGraph.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "A")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), "C")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d1")),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), "E"))
    ))

    val edges2: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 22),
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 52),
      TEdge[Int](3L, 3L, 3L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), 22),
      TEdge[Int](4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 52),
      TEdge[Int](5L, 5L, 5L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), 22)
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

    val expectedEdgesUnion: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 22),
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 52),
      TEdge[Int](3L, 3L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](3L, 3L, 3L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 22),
      TEdge[Int](4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 52),
      TEdge[Int](5L, 2L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](5L, 5L, 5L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), 22)
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


    val expectedEdgesIntersection: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](3L, 3L, 3L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 52)
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

    val expectedEdgesDifference: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42),
      TEdge[Int](3L, 3L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42),
      TEdge[Int](5L, 2L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 42),
      TEdge[Int](5L, 2L, 5L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), 42)

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

    val edges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), 42),
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), 42),
      TEdge[Int](3L, 3L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), 42),
      TEdge[Int](4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), 42)
    ))

    val VEG = VEGraph.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "A")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), "C")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d1")),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), "E"))
    ))

    val edges2: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 22),
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 52),
      TEdge[Int](3L, 3L, 3L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), 22),
      TEdge[Int](4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 52),
      TEdge[Int](5L, 5L, 5L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), 22)
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

    val expectedEdgesDifference: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), 42),
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42),
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), 42),
      TEdge[Int](3L, 3L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42),
      TEdge[Int](3L, 3L, 3L, Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), 42),
      TEdge[Int](4L, 4L, 4L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2019-01-01")), 42)
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

    val edges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)
    ))

    val VEG = VEGraph.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))

    val edges2: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52)
    ))

    val VEG2 = VEGraph.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval,String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))

    val expectedEdgesUnion: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52)
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

    val expectedEdgesDifference: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)
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
    val edges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)
    ))
    val VEG = VEGraph.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))
    val edges2: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52)
    ))
    val VEG2 = VEGraph.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))
    val expectedEdgesUnion: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52)
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

    val expectedEdgesDifference: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)
    ))
    val expectedVEGDifference = VEGraph.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(resultVEGDifference.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(resultVEGDifference.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(resultVEGDifference.getTemporalSequence.collect === expectedVEGDifference.getTemporalSequence.collect)
  }

  test("aggregateMessages - no predicate") {
    testAggregateMessages(empty)
  }

  test("aggregateMessages - edge predicate") {
    testAggregateMessages2(empty)
  }

  test("aggregateMessages - vertex predicate") {
    testAggregateMessages3(empty)
  }

  test("createTemporalNodes aggregateByTime -with structure only") {
    testNodeCreateTemporal2(VEGraph.emptyGraph(true))
  }

  test("createTemporalNodes aggregateByChange -w/o structural") {
    testNodeCreateTemporal3(empty)
  }


  test("createTemporalNodes aggregateByChange -with structural only") {
    testNodeCreateTemporal4(VEGraph.emptyGraph(true))
  }

  test("createAttributeNodes") {
    testNodeCreateStructural(empty)
  }

  test("eSubGraph uncoalesced test"){
    //Regular cases
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Ron")),
      (3L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke"))
    ))
    val edges: RDD[TEdge[Int]] = {
      ProgramContext.sc.parallelize(Array(
        TEdge[Int](1L, 1L, 4L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42),
        TEdge[Int](2L, 3L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 42),
        TEdge[Int](2L, 3L, 5L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2013-01-01")), 42),
        TEdge[Int](3L, 1L, 2L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22),
        TEdge[Int](4L, 5L, 7L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22),
        TEdge[Int](5L, 4L, 8L, Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42),
        TEdge[Int](6L, 4L, 9L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)
      ))
    }

    val g = VEGraph.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

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
    val expectedEdges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](2L, 3L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42)
    ))
    val expectedg = VEGraph.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)
    val actualg = g.esubgraph(pred = tedgeTriplet => tedgeTriplet.interval.intersects(Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01"))), tripletFields = TripletFields.All)

    assert(expectedg.vertices.collect() === actualg.vertices.collect())
    assert(expectedg.edges.collect() === actualg.edges.collect())
    assert(expectedg.getTemporalSequence.collect === actualg.getTemporalSequence.collect)
  }

}
