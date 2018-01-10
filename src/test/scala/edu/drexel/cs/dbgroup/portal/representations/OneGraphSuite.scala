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

class OneGraphSuite extends RepresentationsTestSuite {

  lazy val empty = OneGraph.emptyGraph[String,Int]("Default")

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

  test("aggregateByTime -w/o structural") {
    testNodeCreateTemporal(empty)
  }

  test("aggregateByTime -with structural") {
    testNodeCreateTemporal2(OneGraph.emptyGraph(true))
  }

  test("aggregateByChange -w/o structural") {
    testNodeCreateTemporal3(empty)
  }

  test("aggregateByChange -with structural") {
    testNodeCreateTemporal4(OneGraph.emptyGraph(true))
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

  test("Project") {
    testProject(empty)
  }

  test("from RDD") {
    testFromRDDs(empty)
  }

  test("connected components") {
    testCCs(empty)
  }

  ignore("undirected shortestPath") {
    testShortestPathsUndirected(empty)
  }

  ignore("directed shortestPath") {
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

}
