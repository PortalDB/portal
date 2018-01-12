package edu.drexel.cs.dbgroup.portal.representations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import java.time.LocalDate

import edu.drexel.cs.dbgroup.portal._

import scala.collection.parallel.ParSeq
import org.apache.spark.graphx._
import java.util.Map

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap

import collection.JavaConverters._


class RepresentativeGraphSuite extends RepresentationsTestSuite {
  lazy val empty = RepresentativeGraph.emptyGraph[String,Int]("Default")

  test("slice function") {
    testSlice(empty)
  }

  ignore("temporal select function") {
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
    testNodeCreateTemporal2(RepresentativeGraph.emptyGraph(true))
  }

  test("createTemporalNodes createTemporalByChange -w/o structural") {
    testNodeCreateTemporal3(empty)
  }

  test("createTemporalNodes createTemporalByChange -with structural only") {
    testNodeCreateTemporal4(RepresentativeGraph.emptyGraph(true))
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

  test("Difference Test 2") {
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

    val SGP = RepresentativeGraph.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

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

    val SGP2 = RepresentativeGraph.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resultSGPDifference = SGP.difference(SGP2)

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
    val expectedSGPDifference = RepresentativeGraph.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(resultSGPDifference.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(resultSGPDifference.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(resultSGPDifference.getTemporalSequence.collect === expectedSGPDifference.getTemporalSequence.collect)

  }


  test("Union, intersection and Difference - when there is no overlap between two graphs") {
    testBinary2(empty)
  }

  test("Union,intersection and Difference -when graph.span.start == graph2.span.end") {
    testBinary3(empty)
  }

  test("Project") {
    testProject(empty)
  }

  test("from RDD") {
    testFromRDDs(empty)
  }

/*
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
 */

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

  test("clustering coefficient") {
    testCCoeff(empty)
  }
}
