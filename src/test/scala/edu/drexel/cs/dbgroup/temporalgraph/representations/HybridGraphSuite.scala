package edu.drexel.cs.dbgroup.temporalgraph.representations

import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import java.util.Map
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap

class HybridGraphSuite extends RepresentationsTestSuite {
  lazy val empty = HybridGraph.emptyGraph[String,Int]("Default")

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

  test("createTemporalNodes aggregateByTime -w/o structural") {
    testNodeCreateTemporal(empty)
  }

  test("createTemporalNodes aggregateByTime -with structure only") {
    testNodeCreateTemporal2(HybridGraph.emptyGraph(true))
  }

  test("createTemporalNodes aggregateByChange -w/o structural") {
    testNodeCreateTemporal3(empty)
  }

  test("createTemporalNodes aggregateByChange -with structural only") {
    testNodeCreateTemporal4(HybridGraph.emptyGraph(true))
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

  test("Union, Intersection and Difference") {
    testBinary(empty)
  }

  test("Union, Intersection and Difference - when there is no overlap between two graphs") {
    testBinary2(empty)
  }

  test("Union, Intersection and Difference -when graph.span.start == graph2.span.end") {
    testBinary3(empty)
  }

  test("Union, Intersection and Difference - with structure only") {
    testBinary4(HybridGraph.emptyGraph(true))
  }

  test("Union, Intersection and Difference -when there is no overlap between two graphs and has no attributes") {
    testBinary5(HybridGraph.emptyGraph(true))
  }

  test("Union, Intersection and Difference -when graph.span.start == graph2.span.end and has no attributes") {
    testBinary6(HybridGraph.emptyGraph(true))
  }

  test("Union, Intersection and Difference -when the graphs has higher number of periods than the rundwithds(currently 8)") {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "c")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "e"))
    ))

    val edges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge(3L, 3L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge(4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge(5L, 2L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)
    ))

    val HG = HybridGraph.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2020-01-01")), "A")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2019-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), "C")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d1")),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), "E"))
    ))

    val edges2: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01")), 22),
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 52),
      TEdge(3L, 3L, 3L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), 22),
      TEdge(4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 52),
      TEdge(6L, 5L, 5L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), 22)
    ))

    val HG2 = HybridGraph.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2020-01-01")), "A")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2019-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), "c")),
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "c"+"C")),
      (3L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "C")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d"+"d1")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), "e")),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), "e"+"E")),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), "e"))
    ))

    val expectedEdgesUnion: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01")), 22),
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 52),
      TEdge(3L, 3L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge(3L, 3L, 3L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 22),
      TEdge(4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 52),
      TEdge(5L, 2L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge(6L, 5L, 5L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), 22)
    ))

    val resultHGUnion = HG.union(HG2,(x,y)=>x+y, (x,y)=>math.max(x,y))
    val expectedHGUnion = HybridGraph.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(resultHGUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultHGUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultHGUnion.getTemporalSequence.collect === expectedHGUnion.getTemporalSequence.collect)

    val resultHGIntersection = HG.intersection(HG2,(x,y)=>x+y, (x,y)=>math.max(x,y))

    val expectedVerticesIntersection: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "c"+ "C")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d"+ "d1")),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), "e"+ "E"))
    ))

    val expectedEdgesIntersection: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge(3L, 3L, 3L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge(4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 52)
    ))
    val expectedHGIntersection = HybridGraph.fromRDDs(expectedVerticesIntersection, expectedEdgesIntersection, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(resultHGIntersection.vertices.collect.toSet === expectedVerticesIntersection.collect.toSet)
    assert(resultHGIntersection.edges.collect.toSet === expectedEdgesIntersection.collect.toSet)
    assert(resultHGIntersection.getTemporalSequence.collect === expectedHGIntersection.getTemporalSequence.collect)

    val resultHGDiffenrence = HG.difference(HG2)

    val expectedVerticesDifference: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), "c")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), "e")),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), "e"))
    ))

    val expectedEdgesDifference: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42),
      TEdge(3L, 3L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42),
      TEdge(5L, 2L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 42),
      TEdge(5L, 2L, 5L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), 42)
    ))

    val expectedHGDifference = HybridGraph.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, "Default", StorageLevel.MEMORY_ONLY_SER)
    assert(resultHGDiffenrence.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(resultHGDiffenrence.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(resultHGDiffenrence.getTemporalSequence.collect === expectedHGDifference.getTemporalSequence.collect)
  }

  test("Difference 2 - wtih structure only") {
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

    val HG = HybridGraph.fromRDDs(users, edges, true, StorageLevel.MEMORY_ONLY_SER)

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

    val HG2 = HybridGraph.fromRDDs(users2, edges2, true, StorageLevel.MEMORY_ONLY_SER)
    val resultHGIntersection = HG.difference(HG2)

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

    val expectedHGDifference = HybridGraph.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, true, StorageLevel.MEMORY_ONLY_SER)

    assert(resultHGIntersection.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(resultHGIntersection.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(resultHGIntersection.getTemporalSequence.collect === expectedHGDifference.getTemporalSequence.collect)
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
