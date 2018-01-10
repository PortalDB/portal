package edu.drexel.cs.dbgroup.portal.representations

import java.time.LocalDate

import edu.drexel.cs.dbgroup.portal._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Map

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap

class OneGraphColumnSuite extends RepresentationsTestSuite {
  lazy val empty = OneGraphColumn.emptyGraph[String,Int]("Default")

  test("slice function") {
    testSlice(empty)
  }

  test("temporal select function") {
    testTemporalSelect(empty)
  }

  test("structural select function") {
    testStructuralSelect(empty)
  }

  test("getSnapshot function") {
    testGetSnapshot(empty)
  }

  test("createTemporalNodes createTemporalByTime -w/o structural") {
    testNodeCreateTemporal(empty)
  }

  test("createTemporalNodes createTemporalByTime -with structure only") {
    testNodeCreateTemporal2(OneGraphColumn.emptyGraph(true))
  }

  test("createTemporalNodes createTemporalByChange -w/o structural") {
    testNodeCreateTemporal3(empty)
  }

  test("createTemporalNodes createTemporalByChange -with structural only") {
    testNodeCreateTemporal4(OneGraphColumn.emptyGraph(true))
  }

  test("createAttributeNodes") {
    testNodeCreateStructural(empty)
  }

  test("size, getTemporalSequence.collect") {
    testGetTemporalSequence(empty)
  }

  test("degree") {
    testDegrees(empty)
  }

  test("Union ,Intersection and Difference") {
    testBinary(empty)
  }

  test("Union, intersection and Difference - when there is no overlap between two graphs") {
    testBinary2(empty)
  }

  test("Union,intersection and Difference -when graph.span.start == graph2.span.end") {
    testBinary3(empty)
  }

  test("Union, Intersection and Difference - with structure only") {
    testBinary4(OneGraphColumn.emptyGraph(true))
  }

  test("Union, Intersection and Difference -when there is no overlap between two graphs and has null attributes") {
    testBinary5(OneGraphColumn.emptyGraph(true))
  }

  test("Union, Intersection and Difference -when graph.span.start == graph2.span.end and has null attributes") {
    testBinary6(OneGraphColumn.emptyGraph(true))
  }

  test("Difference 2 - with structure only") {
    val users: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), true)),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), true))
    ))

    val edges: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), true),
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), true),
      TEdge(3L, 3L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), true),
      TEdge(4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2019-01-01")), true)
    ))

    val OGC = OneGraphColumn.fromRDDs(users, edges, true, StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), true)),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), true))
    ))

    val edges2: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), true),
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), true),
      TEdge(3L, 3L, 3L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), true),
      TEdge(4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true),
      TEdge(6L, 5L, 5L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), true)
    ))

    val OGC2 = OneGraphColumn.fromRDDs(users2, edges2, true, StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesDifference: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), true)),
      (1L, (Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), true)),
      (4L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2019-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2019-01-01")), true))
    ))

    val expectedEdgesDifference: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true),
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), true),
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), true),
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), true),
      TEdge(3L, 3L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), true),
      TEdge(3L, 3L, 3L, Interval(LocalDate.parse("2018-01-01"), LocalDate.parse("2019-01-01")), true),
      TEdge(4L, 4L, 4L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2019-01-01")), true)
    ))

    val expectedOGCDifference = OneGraphColumn.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, true, StorageLevel.MEMORY_ONLY_SER)
    val resultOGCDifference = OGC.difference(OGC2)

    assert(resultOGCDifference.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(resultOGCDifference.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(resultOGCDifference.getTemporalSequence.collect === expectedOGCDifference.getTemporalSequence.collect)
  }

  test("Project") {
    testProject(empty)
  }

  test("from RDD") {
    testFromRDDs(empty)
  }

  test("connected components") {
    testCCs(empty)
  }

  test("undirected shortestPath") {
    testShortestPathsUndirected(empty)
  }

  test("directed shortestPath") {
    testShortestPathsDirected(empty)
  }

  test("directed pagerank") {
    testPageRankDirected(empty)
  }

  test("undirected pagerank") {
    testPageRankUndirected(empty)
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

  test("triangle count") {
    testTriangleCount(empty)
  }

  test("clusterring coefficient") {
    testCCoeff(empty)
  }
}
