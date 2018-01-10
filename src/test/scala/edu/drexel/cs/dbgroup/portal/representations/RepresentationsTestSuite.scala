package edu.drexel.cs.dbgroup.portal.representations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import java.time.LocalDate

import edu.drexel.cs.dbgroup.portal._

import scala.collection.parallel.ParSeq
import org.apache.spark.graphx._
import java.util.Map
import scala.reflect.ClassTag

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap

import collection.JavaConverters._
import scala.reflect._
import scala.reflect.runtime.universe._

abstract class RepresentationsTestSuite extends FunSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
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

  protected override def afterAll {
    ProgramContext.sc.stop
    ProgramContext.sc = null
  }

  protected def testSlice(ge: TGraphNoSchema[String,Int]):Unit = {
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
    val edges: RDD[TEdge[Int]] = getTestEdges_Int_1b()
    val g = ge.fromRDDs(users, edges, "Default")

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke"))
    ))
    val expectedEdges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 4L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22),
      TEdge[Int](2L, 3L, 5L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), 22),
      TEdge[Int](3L, 1L, 2L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), 22),
      TEdge[Int](6L, 4L, 9L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)
    ))
    val expectedg =  ge.fromRDDs(expectedUsers, expectedEdges, "Default")
    //behavior is different for materialized and unmaterialized graph
    //although the result should be the same
    g.materialize
    var actualg = g.slice(sliceInterval)

    assert(expectedg.vertices.collect() === actualg.vertices.collect())
    assert(expectedg.edges.collect() === actualg.edges.collect())
    assert(expectedg.getTemporalSequence.collect === actualg.getTemporalSequence.collect)
    info("regular cases passed")

    //When interval is completely outside the graph
    val sliceInterval2 = (Interval(LocalDate.parse("2001-01-01"), LocalDate.parse("2003-01-01")))
    val actualg2 = g.slice(sliceInterval2)
    assert(actualg2.vertices.collect() === ge.vertices.collect())
    assert(actualg2.edges.collect() === ge.edges.collect())
    assert(actualg2.getTemporalSequence.collect === Seq[Interval]())
    info("interval completely outside the graph passed")

    //When the graph is empty
    val actualg3 = ge.slice(sliceInterval2)
    assert(actualg3.vertices.collect() === ge.vertices.collect())
    assert(actualg3.edges.collect() === ge.edges.collect())
    assert(actualg3.getTemporalSequence.collect === Seq[Interval]())
    info("empty graph passed")

  }

  def testTemporalSelect(ge: TGraphNoSchema[String,Int]):Unit = {
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
    val edges: RDD[TEdge[Int]] = getTestEdges_Int_1b
    val g = ge.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))
    ))
    val expectedEdges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array[TEdge[Int]]())
    val expectedg = ge.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)
    var selectFunction = (id: VertexId, attr: String, x: Interval) => x.equals(Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")))
    var actualg = g.vsubgraph(selectFunction)

    assert(expectedg.vertices.collect() === actualg.vertices.collect())
    assert(expectedg.edges.collect() === actualg.edges.collect())
    assert(expectedg.getTemporalSequence.collect === actualg.getTemporalSequence.collect)
    info("regular cases passed")

    //When interval is completely outside the graph
    selectFunction = (vid: VertexId, attr: String, x: Interval) => x.equals(Interval(LocalDate.parse("2001-01-01"), LocalDate.parse("2003-01-01")))
    val actualg2 = g.vsubgraph(selectFunction)
    assert(actualg2.vertices.collect() === ge.vertices.collect())
    assert(actualg2.edges.collect() === ge.edges.collect())
    assert(actualg2.getTemporalSequence.collect === Seq[Interval]())
    info("interval completely outside the graph passed")

    //When the graph is empty
    val actualg3 = ge.vsubgraph(selectFunction)
    assert(actualg3.vertices.collect() === ge.vertices.collect())
    assert(actualg3.edges.collect() === ge.edges.collect())
    assert(actualg3.getTemporalSequence.collect === Seq[Interval]())
    info("empty graph passed")
  }

  def testStructuralSelect(ge: TGraphNoSchema[String,Int]):Unit = {
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
    val edges: RDD[TEdge[Int]] = getTestEdges_Int_1a()
    val g = ge.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

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
      TEdge[Int](2L, 3L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42),
      TEdge[Int](5L, 4L, 8L, Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)
    ))
    val expectedg = ge.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)
    val actualg = g.esubgraph(pred = tedgeTriplet => tedgeTriplet.srcId > 2 && tedgeTriplet.attr==42)

    assert(expectedg.vertices.collect() === actualg.vertices.collect())
    assert(expectedg.edges.collect() === actualg.edges.collect())
    assert(expectedg.getTemporalSequence.collect === actualg.getTemporalSequence.collect)

    val expectedUsers2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro"))
    ))
    val expectedEdges2: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](4L, 5L, 7L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22),
      TEdge[Int](5L, 4L, 8L, Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)
    ))
    val expectedg2 = ge.fromRDDs(expectedUsers2, expectedEdges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    val actualg2 = g.vsubgraph(pred = (id: VertexId, attrs: String,interval: Interval) => id > 3 && attrs != "Ke")

    assert(expectedg2.vertices.collect.toSet === actualg2.vertices.collect.toSet)
    assert(expectedg2.edges.collect.toSet === actualg2.edges.collect.toSet)
    assert(expectedg2.getTemporalSequence.collect === actualg2.getTemporalSequence.collect)

    val expectedUsers3: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana")),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro"))
    ))
    val expectedEdges3: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](5L, 4L, 8L, Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)
    ))
    val expectedg3 = ge.fromRDDs(expectedUsers3, expectedEdges3, "Default", StorageLevel.MEMORY_ONLY_SER)
    val actualg3 = g.vsubgraph(pred = (id: VertexId, attrs: String,interval: Interval) => id > 3 && attrs != "Ke").esubgraph(pred = tedgeTriplet => tedgeTriplet.srcId > 2 && tedgeTriplet.attr==42)
    assert(expectedg3.vertices.collect() === actualg3.vertices.collect())
    assert(expectedg3.edges.collect() === actualg3.edges.collect())
    assert(expectedg3.getTemporalSequence.collect === actualg3.getTemporalSequence.collect)
  }

  def testGetSnapshot(ge: TGraphNoSchema[String,Int]): Unit = {
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
    val edges: RDD[TEdge[Int]] = getTestEdges_Int_1a()
    val g = ge.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedUsers = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (3L, "Ron"),
      (4L, "Julia"),
      (5L, "Vera"),
      (6L, "Halima")
    ))
    val expectedEdges = ProgramContext.sc.parallelize(Array(
      Edge(1L, 4L, (1L, 42)),
      Edge(3L, 5L, (2L, 42))
    ))
    var actualg = g.getSnapshot((LocalDate.parse("2012-07-01")))

    assert(expectedUsers.collect.toSet === actualg.vertices.collect.toSet)
    assert(expectedEdges.collect.toSet === actualg.edges.collect.toSet)
  }

  def testNodeCreateTemporal(ge: TGraphNoSchema[String,Int]): Unit = {
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
    val edges: RDD[TEdge[Int]] = getTestEdges_Int_1a().union(getTestEdges_Int_2())
    val g = ge.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resolution1Month = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2011-02-01"))
    val resolution3Years = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2014-01-01"))

    val actualg = g.createTemporalNodes(new TimeSpec(resolution3Years), Always(), Always(), (attr1, attr2) => attr2, (attr1, attr2) => attr2)

    val expectedVertices: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "John")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2012-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2018-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Mike"))
    ))

    val expectedEdges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 4L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42)
    ))
    val expectedg = ge.fromRDDs(expectedVertices, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedVertices.collect().toSet === actualg.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualg.edges.collect().toSet)
    assert(expectedg.getTemporalSequence.collect === actualg.getTemporalSequence.collect)

    val actualg2 = g.createTemporalNodes(new TimeSpec(resolution3Years), Always(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => Math.max(attr1, attr2))

    val expectedVertices2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "John")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2012-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2018-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Mike"))
    ))

    val expectedEdges2: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 4L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42),
      TEdge[Int](7L, 4L, 6L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22),
      TEdge[Int](8L, 4L, 6L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 72)
    ))
    val expectedg2 = ge.fromRDDs(expectedVertices2, expectedEdges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedVertices2.collect().toSet === actualg2.vertices.collect().toSet)
    assert(expectedEdges2.collect().toSet === actualg2.edges.collect().toSet)
    assert(expectedg2.getTemporalSequence.collect === actualg2.getTemporalSequence.collect)
  }

  def testNodeCreateTemporal2(ge: TGraphNoSchema[StructureOnlyAttr,StructureOnlyAttr]): Unit = {
    val users: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), true)),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2018-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), true)),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), true)),
      (8L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), true)),
      (9L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), true)),
      (6L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), true))

    ))
    val edges: RDD[TEdge[StructureOnlyAttr]] = getTestEdges_Bool_1()
    val g = ge.fromRDDs(users, edges, true, StorageLevel.MEMORY_ONLY_SER)

    val resolution1Month = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2011-02-01"))
    val resolution3Years = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2014-01-01"))
    val longerString = (a: String, b: String) =>
      if (a.length > b.length) a else if (a.length < b.length) b else if (a.compareTo(b) > 0) a else b

    val actualg = g.createTemporalNodes(new TimeSpec(resolution3Years), Always(), Always(), (name1, name2) => name2, (count1, count2) => count2)

    val expectedVertices: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2012-01-01")), true)),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2018-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), true))
    ))

    val expectedEdges: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge[StructureOnlyAttr](1L, 1L, 4L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)
    ))
    val expectedg = ge.fromRDDs(expectedVertices, expectedEdges, true, StorageLevel.MEMORY_ONLY_SER)

    assert(expectedVertices.collect().toSet === actualg.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualg.edges.collect().toSet)
    assert(expectedg.getTemporalSequence.collect === actualg.getTemporalSequence.collect)

    val actualg2 = g.createTemporalNodes(new TimeSpec(resolution3Years), Always(), Exists(), (name1, name2) => name1, (count1, count2) => count1)

    val expectedVertices2: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2012-01-01")), true)),
      (4L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2018-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (6L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), true))
    ))

    val expectedEdges2: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge[StructureOnlyAttr](1L, 1L, 4L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true),
      TEdge[StructureOnlyAttr](7L, 4L, 6L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true),
      TEdge[StructureOnlyAttr](8L, 4L, 6L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)
    ))
    val expectedg2 = ge.fromRDDs(expectedVertices2, expectedEdges2, true, StorageLevel.MEMORY_ONLY_SER)

    assert(expectedVertices2.collect().toSet === actualg2.vertices.collect.toSet)
    assert(expectedEdges2.collect().toSet === actualg2.edges.collect().toSet)
    assert(expectedg2.getTemporalSequence.collect === actualg2.getTemporalSequence.collect)
  }

  def testNodeCreateTemporal3(ge: TGraphNoSchema[String,Int]): Unit = {
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
    val edges: RDD[TEdge[Int]] = getTestEdges_Int_1a().union(//part 2 is slightly different here
      ProgramContext.sc.parallelize(Array(
        TEdge[Int](7L, 4L, 6L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 22),
        TEdge[Int](8L, 4L, 6L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), 72)
      ))
    )

    val g = ge.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)
    val actualg = g.createTemporalNodes(new ChangeSpec(1), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)
    val expectedg = ge.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(users.collect().toSet === actualg.vertices.collect().toSet)
    assert(edges.collect().toSet === actualg.edges.collect().toSet)
    assert(expectedg.getTemporalSequence.collect === actualg.getTemporalSequence.collect)

    val actualg2 = g.createTemporalNodes(new ChangeSpec(2), Exists(), Exists(),  (attr1, attr2) => attr1, (attr1, attr2) => attr1)
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
    val expectedEdges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L ,1L, 4L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), 42),
      TEdge[Int](2L, 3L, 5L, Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2013-01-01")), 42),
      TEdge[Int](3L, 1L, 2L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2017-01-01")), 22),
      TEdge[Int](4L, 5L, 7L, Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), 22),
      TEdge[Int](5L, 4L, 8L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), 42),
      TEdge[Int](6L, 4L, 9L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 22),
      TEdge[Int](7L, 4L, 6L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 22),
      TEdge[Int](8L, 4L, 6L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2013-01-01")), 72)
    ))
    val expectedg2 = ge.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedUsers.collect().toSet === actualg2.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualg2.edges.collect().toSet)
    assert(expectedg2.getTemporalSequence.collect === actualg2.getTemporalSequence.collect)

    val actualg3 = g.createTemporalNodes(new ChangeSpec(2), Always(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)

    val expectedUsers3: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2013-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), "Vera")),
      (6L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), "Halima")),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana"))
    ))
    val expectedEdges3: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 4L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 42),
      TEdge[Int](2L, 3L, 5L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2013-01-01")), 42),
      TEdge[Int](3L, 1L, 2L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), 22),
      TEdge[Int](7L, 4L, 6L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 22)
    ))
    val expectedg3 = ge.fromRDDs(expectedUsers3, expectedEdges3, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedUsers3.collect().toSet === actualg3.vertices.collect().toSet)
    assert(expectedEdges3.collect().toSet === actualg3.edges.collect().toSet)
    assert(expectedg3.getTemporalSequence.collect === actualg3.getTemporalSequence.collect)
  }

  def testNodeCreateTemporal4(ge: TGraphNoSchema[StructureOnlyAttr,StructureOnlyAttr]): Unit = {
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
    val edges: RDD[TEdge[StructureOnlyAttr]] = getTestEdges_Bool_1a

    val g = ge.fromRDDs(users, edges, true, StorageLevel.MEMORY_ONLY_SER)
    val actualg = g.createTemporalNodes(new ChangeSpec(1), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)
    val expectedg = ge.fromRDDs(users, edges, true, StorageLevel.MEMORY_ONLY_SER)

    assert(users.collect().toSet === actualg.vertices.collect().toSet)
    assert(edges.collect().toSet === actualg.edges.collect().toSet)
    assert(expectedg.getTemporalSequence.collect === actualg.getTemporalSequence.collect)

    val actualg2 = g.createTemporalNodes(new ChangeSpec(2), Exists(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)
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
    val expectedEdges: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge[StructureOnlyAttr](1L, 1L, 4L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), true),
      TEdge[StructureOnlyAttr](2L, 3L, 5L, Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2013-01-01")), true),
      TEdge[StructureOnlyAttr](3L, 1L, 2L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2017-01-01")), true),
      TEdge[StructureOnlyAttr](4L, 5L, 7L, Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), true),
      TEdge[StructureOnlyAttr](5L, 4L, 8L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), true),
      TEdge[StructureOnlyAttr](6L, 4L, 9L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), true),
      TEdge[StructureOnlyAttr](7L, 4L, 6L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), true)
    ))
    val expectedg2 = ge.fromRDDs(expectedUsers, expectedEdges, true, StorageLevel.MEMORY_ONLY_SER)

    assert(expectedUsers.collect().toSet === actualg2.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualg2.edges.collect().toSet)
    assert(expectedg2.getTemporalSequence.collect === actualg2.getTemporalSequence.collect)

    val actualg3 = g.createTemporalNodes(new ChangeSpec(2), Always(), Exists(), (attr1, attr2) => attr1, (attr1, attr2) => attr1)

    val expectedUsers3: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2017-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2013-01-01")), true)),
      (4L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2017-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2015-01-01")), true)),
      (6L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), true)),
      (7L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), true))
    ))
    val expectedEdges3: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge[StructureOnlyAttr](1L, 1L, 4L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), true),
      TEdge[StructureOnlyAttr](2L, 3L, 5L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2013-01-01")), true),
      TEdge[StructureOnlyAttr](3L, 1L, 2L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), true),
      TEdge[StructureOnlyAttr](7L, 4L, 6L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), true)
    ))
    val expectedg3 = ge.fromRDDs(expectedUsers3, expectedEdges3, true, StorageLevel.MEMORY_ONLY_SER)

    assert(expectedUsers3.collect().toSet === actualg3.vertices.collect().toSet)
    assert(expectedUsers3.collect().toSet === actualg3.vertices.collect().toSet)
    assert(expectedEdges3.collect().toSet === actualg3.edges.collect().toSet)
    assert(expectedg3.getTemporalSequence.collect === actualg3.getTemporalSequence.collect)
  }

  def testNodeCreateStructural(ge: TGraphNoSchema[String,Int]): Unit = {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2012-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2013-01-01")), "ab")),
      (3L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), "abc")),
      (4L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), "abcd")),
      (5L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), "abcde")),
      (6L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2017-01-01")), "abcdef")),
      (7L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2018-01-01")), "abcdefg"))

    ))

    val edges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L,2L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), 40),
      TEdge[Int](2L, 2L,3L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), 50),
      TEdge[Int](3L, 3L,4L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 60),
      TEdge[Int](4L, 4L,5L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), 70),
      TEdge[Int](5L, 5L,6L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01")), 80),
      TEdge[Int](6L, 6L,7L, Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 90)
    ))

    val g = ge.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val longerString = (a: String, b: String) =>
      if (a.length > b.length) a else if (a.length < b.length) b else if (a.compareTo(b) > 0) a else b

    val actualg = g.createAttributeNodes( (name1, name2) => longerString(name1, name2))((vid, attr1) => if (attr1.length < 5) 1L else 2L)

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), "a")),
      (1L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), "ab")),
      (1L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), "abc")),
      (1L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), "abcd")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), "abcde")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01")), "abcdef")),
      (2L, (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2018-01-01")), "abcdefg"))

    ))

    val expectedEdges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L,1L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), 40),
      TEdge[Int](2L, 1L,1L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), 50),
      TEdge[Int](3L, 1L,1L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 60),
      TEdge[Int](4L, 1L,2L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), 70),
      TEdge[Int](5L, 2L,2L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01")), 80),
      TEdge[Int](6L, 2L,2L, Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 90)
    ))
    val expectedg = ge.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedUsers.collect().toSet === actualg.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualg.edges.collect().toSet)
    assert(expectedg.getTemporalSequence.collect === actualg.getTemporalSequence.collect)
  }

  def testGetTemporalSequence(ge: TGraphNoSchema[String,Int]): Unit = {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron"))
    ))
    val edges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2012-01-01")), 42),
      TEdge[Int](2L, 1L, 2L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)
    ))
    val g = ge.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resultInterval = g.size()
    val expectedInterval = Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2018-01-01"))
    assert(resultInterval === expectedInterval)

    val resultSeq = g.getTemporalSequence.collect

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

  def testDegrees(ge: TGraphNoSchema[String,Int]): Unit = {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron"))
    ))
    val edges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](2L, 1L, 2L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2016-01-01")), 22),
      TEdge[Int](3L, 1L, 2L, Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)
    ))
    val g = ge.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resultDegree = g.degree

    val expectedDegree = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 1)),
      (1L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 2)),
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2017-01-01")), 1)),
      (2L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2017-01-01")), 1)),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 1))
    ))

    assert(expectedDegree.collect.toSet === resultDegree.collect.toSet)
  }

  def testBinary(ge: TGraphNoSchema[String,Int]): Unit = {
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

    val g = ge.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

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
      TEdge[Int](6L, 5L, 5L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), 22)
    ))

    val g2 = ge.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "A")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), "c")),
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2018-01-01")), "C")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d")),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), "e")),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), "E")),
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
      TEdge[Int](6L, 5L, 5L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), 22)
    ))

    val resultgUnion = g.union(g2, (x,y)=> {if (x < y) x else y} , (x,y)=>math.max(x,y))
    val expectedgUnion = ge.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(resultgUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultgUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultgUnion.getTemporalSequence.collect === expectedgUnion.getTemporalSequence.collect)

    val resultgIntersection = g.intersection(g2, (x,y)=> { if (x < y) x else y} , (x,y)=>math.max(x,y))

    val expectedVerticesIntersection: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "C")),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d")),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")), "E"))
    ))

    val expectedEdgesIntersection: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](3L, 3L, 3L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 52)
    ))
    val expectedgIntersection = ge.fromRDDs(expectedVerticesIntersection, expectedEdgesIntersection, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resultgDifference = g.difference(g2)

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
    val expectedgDifference = ge.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(resultgDifference.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(resultgDifference.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(resultgDifference.getTemporalSequence.collect === expectedgDifference.getTemporalSequence.collect)

    assert(resultgIntersection.vertices.collect.toSet === expectedVerticesIntersection.collect.toSet)
    assert(resultgIntersection.edges.collect.toSet === expectedEdgesIntersection.collect.toSet)
    assert(resultgIntersection.getTemporalSequence.collect === expectedgIntersection.getTemporalSequence.collect)
  }

  def testBinary2(ge: TGraphNoSchema[String,Int]): Unit = {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b"))
    ))

    val edges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)
    ))

    val g = ge.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))

    val edges2: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52)
    ))

    val g2 = ge.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b")),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))

    val expectedEdgesUnion: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52)
    ))

    val expectedgUnion = ge.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resultgUnion = g.union(g2, (x,y)=>x+y , (x,y)=>math.max(x,y))

    assert(resultgUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultgUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultgUnion.getTemporalSequence.collect === expectedgUnion.getTemporalSequence.collect)

    val resultgIntersection = g.intersection(g2, (x,y)=>x+y , (x,y)=>math.max(x,y))

    assert(resultgIntersection.vertices.collect.toSet === ge.vertices.collect.toSet)
    assert(resultgIntersection.edges.collect.toSet === ge.edges.collect.toSet)
    assert(resultgIntersection.getTemporalSequence.collect === Seq[Interval]())

    val resultgDifference = g.difference(g2)

    val expectedVerticesDifference: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b"))

    ))

    val expectedEdgesDifference: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)
    ))
    val expectedgDifference = ge.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(resultgDifference.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(resultgDifference.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(resultgDifference.getTemporalSequence.collect === expectedgDifference.getTemporalSequence.collect)
  }

  def testBinary3(ge: TGraphNoSchema[String,Int]): Unit = {
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b"))
    ))
    val edges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)
    ))
    val g = ge.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "b1")),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), "C"))
    ))
    val edges2: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), 52)
    ))
    val g2 = ge.fromRDDs(users2, edges2, "Default", StorageLevel.MEMORY_ONLY_SER)

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
    val expectedgUnion = ge.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, "Default", StorageLevel.MEMORY_ONLY_SER)

    val resultgUnion = g.union(g2, (x,y)=>x+y , (x,y)=>math.max(x,y))

    assert(resultgUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultgUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultgUnion.getTemporalSequence.collect === expectedgUnion.getTemporalSequence.collect)

    val resultgIntersection = g.intersection(g2, (x,y)=>x+y , (x,y)=>math.max(x,y))

    assert(resultgIntersection.vertices.collect.toSet === ge.vertices.collect.toSet)
    assert(resultgIntersection.edges.collect.toSet === ge.edges.collect.toSet)
    assert(resultgIntersection.getTemporalSequence.collect === Seq[Interval]())

    val resultgDifference = g.difference(g2)

    val expectedVerticesDifference: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "a")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "b"))

    ))

    val expectedEdgesDifference: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)
    ))
    val expectedgDifference = ge.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(resultgDifference.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(resultgDifference.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(resultgDifference.getTemporalSequence.collect === expectedgDifference.getTemporalSequence.collect)
  }

  def testBinary4(ge: TGraphNoSchema[StructureOnlyAttr,StructureOnlyAttr]): Unit = {
    val users: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true))
    ))

    val edges: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true),
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true),
      TEdge(3L, 3L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true),
      TEdge(4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true),
      TEdge(5L, 2L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)
    ))

    val g = ge.fromRDDs(users, edges, true, StorageLevel.MEMORY_ONLY_SER)

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

    val g2 = ge.fromRDDs(users2, edges2, true, StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),true)),
      (1L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),true)),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),true)),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),true)),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),true)),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),true))
    ))

    val expectedEdgesUnion: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),true),
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),true),
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),true),
      TEdge(3L, 3L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),true),
      TEdge(4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),true),
      TEdge(5L, 2L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),true),
      TEdge(6L, 5L, 5L, Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")),true)
    ))

    val expectedgUnion = ge.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, true, StorageLevel.MEMORY_ONLY_SER)
    val resultgUnion = g.union(g2, (x,y)=>x , (x,y)=>x)

    assert(resultgUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultgUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultgUnion.getTemporalSequence.collect === expectedgUnion.getTemporalSequence.collect)

    val resultgIntersection = g.intersection(g2, (x,y)=>x , (x,y)=>x)

    val expectedVerticesIntersection: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (3L, (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")),true)),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),true)),
      (5L, (Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")),true))
    ))

    val expectedEdgesIntersection: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(3L, 3L, 3L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")),true),
      TEdge(4L, 4L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),true)
    ))
    val expectedgIntersection = ge.fromRDDs(expectedVerticesIntersection, expectedEdgesIntersection, true, StorageLevel.MEMORY_ONLY_SER)

    assert(resultgIntersection.vertices.collect.toSet === expectedVerticesIntersection.collect.toSet)
    assert(resultgIntersection.edges.collect.toSet === expectedEdgesIntersection.collect.toSet)
    assert(resultgIntersection.getTemporalSequence.collect === expectedgIntersection.getTemporalSequence.collect)

    val resultgDifference = g.difference(g2)

    val expectedVerticesDifference: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), true)),
      (5L, (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), true))

    ))

    val expectedEdgesDifference: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true),
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), true),
      TEdge(3L, 3L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), true),
      TEdge(5L, 2L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), true),
      TEdge(5L, 2L, 5L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), true)

    ))
    val expectedgDifference = ge.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, true, StorageLevel.MEMORY_ONLY_SER)

    assert(resultgDifference.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(resultgDifference.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(resultgDifference.getTemporalSequence.collect === expectedgDifference.getTemporalSequence.collect)
  }

  def testBinary5(ge: TGraphNoSchema[StructureOnlyAttr,StructureOnlyAttr]): Unit = {
    val users: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true))
    ))

    val edges: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)
    ))

    val g = ge.fromRDDs(users, edges, true, StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), true))
    ))

    val edges2: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), true)
    ))

    val g2 = ge.fromRDDs(users2, edges2, true, StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),true)),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),true)),
      (2L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),true)),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),true))
    ))

    val expectedEdgesUnion: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),true),
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),true)
    ))

    val resultgUnion = g.union(g2, (x,y)=>x , (x,y)=>x)
    val expectedgUnion = ge.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, true, StorageLevel.MEMORY_ONLY_SER)

    assert(resultgUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultgUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultgUnion.getTemporalSequence.collect === expectedgUnion.getTemporalSequence.collect)

    val resultgIntersection = g.intersection(g2, (x,y)=>x , (x,y)=>x)

    assert(resultgIntersection.vertices.collect.toSet === ge.vertices.collect.toSet)
    assert(resultgIntersection.edges.collect.toSet === ge.edges.collect.toSet)
    assert(resultgIntersection.getTemporalSequence.collect === Seq[Interval]())


    val resultgDifference = g.difference(g2)

    val expectedVerticesDifference: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true))
    ))

    val expectedEdgesDifference: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)
    ))
    val expectedgDifference = ge.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, true, StorageLevel.MEMORY_ONLY_SER)

    assert(resultgDifference.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(resultgDifference.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(resultgDifference.getTemporalSequence.collect === expectedgDifference.getTemporalSequence.collect)
  }

  def testBinary6(ge: TGraphNoSchema[StructureOnlyAttr,StructureOnlyAttr]): Unit = {
    val users: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true))
    ))

    val edges: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)
    ))

    val g = ge.fromRDDs(users, edges, true, StorageLevel.MEMORY_ONLY_SER)

    val users2: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), true)),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), true))
    ))

    val edges2: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")), true)
    ))

    val g2 = ge.fromRDDs(users2, edges2, true, StorageLevel.MEMORY_ONLY_SER)

    val expectedVerticesUnion: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),true)),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")),true)),
      (3L, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),true))
    ))

    val expectedEdgesUnion: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")),true),
      TEdge(2L, 2L, 3L, Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2018-01-01")),true)
    ))

    val resultgUnion = g.union(g2, (x,y)=>x , (x,y)=>x)
    val expectedgUnion = ge.fromRDDs(expectedVerticesUnion, expectedEdgesUnion, true, StorageLevel.MEMORY_ONLY_SER)

    assert(resultgUnion.vertices.collect.toSet === expectedVerticesUnion.collect.toSet)
    assert(resultgUnion.edges.collect.toSet === expectedEdgesUnion.collect.toSet)
    assert(resultgUnion.getTemporalSequence.collect === expectedgUnion.getTemporalSequence.collect)

    val resultgIntersection = g.intersection(g2, (x,y)=>x , (x,y)=>x)

    assert(resultgIntersection.vertices.collect.toSet === ge.vertices.collect.toSet)
    assert(resultgIntersection.edges.collect.toSet === ge.edges.collect.toSet)
    assert(resultgIntersection.getTemporalSequence.collect === Seq[Interval]())

    val resultgDifference = g.difference(g2)

    val expectedVerticesDifference: RDD[(VertexId, (Interval, StructureOnlyAttr))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true))
    ))

    val expectedEdgesDifference: RDD[TEdge[StructureOnlyAttr]] = ProgramContext.sc.parallelize(Array(
      TEdge(1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), true)
    ))
    val expectedgDifference = ge.fromRDDs(expectedVerticesDifference, expectedEdgesDifference, true, StorageLevel.MEMORY_ONLY_SER)

    assert(resultgDifference.vertices.collect.toSet === expectedVerticesDifference.collect.toSet)
    assert(resultgDifference.edges.collect.toSet === expectedEdgesDifference.collect.toSet)
    assert(resultgDifference.getTemporalSequence.collect === expectedgDifference.getTemporalSequence.collect)
  }

  def testProject(ge: TGraphNoSchema[String,Int]): Unit = {
    //Checks for projection and coalescing of vertices and edges
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "B")),
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), "b")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "c")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "d"))
    ))
    val edges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2012-01-01")), 4),
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), -4),
      TEdge[Int](2L, 1L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 2)
    ))
    val g = ge.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val actualg = g.emap(tedge => tedge.attr * tedge.attr).vmap((vertex,intv, name) => name.toUpperCase, "Default")

    val expectedVertices: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2016-01-01")), "B")),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "C")),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), "D"))
    ))
    val expectedEdges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 16),
      TEdge[Int](2L, 1L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 4)
    ))
    val expectedg = ge.fromRDDs(expectedVertices, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)

    assert(expectedVertices.collect().toSet === actualg.vertices.collect().toSet)
    assert(expectedEdges.collect().toSet === actualg.edges.collect().toSet)
    assert(actualg.getTemporalSequence.collect === expectedg.getTemporalSequence.collect)
  }

  def testFromRDDs(ge: TGraphNoSchema[String,Int]): Unit = {
    //Checks if the fromRDD function creates the correct graphs. Graphs variable is protected so to get the graphs, we use getSnapshot function
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron"))
    ))
    val edges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2012-01-01")), 42),
      TEdge[Int](2L, 1L, 2L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)
    ))
    val g = ge.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    //should be empty
    val graph1 = g.getSnapshot(LocalDate.parse("2008-12-01"))
    val graph2 = g.getSnapshot(LocalDate.parse("2018-01-01"))

    assert(graph1.vertices.isEmpty())
    assert(graph1.edges.isEmpty())

    assert(graph2.vertices.isEmpty())
    assert(graph2.edges.isEmpty())

    //not empty
    val graph3 = g.getSnapshot(LocalDate.parse("2009-01-01"))
    val expectedUsers3 = ProgramContext.sc.parallelize(Array(
      (3L, "Ron")
    ))
    assert(expectedUsers3.collect.toSet === graph3.vertices.collect.toSet)
    assert(graph3.edges.isEmpty())

    val graph4 = g.getSnapshot(LocalDate.parse("2010-01-01"))
    val expectedUsers4 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (3L, "Ron")
    ))
    val expectedEdges4 = ProgramContext.sc.parallelize(Array(
      Edge(1L, 3L, (1L, 42))
    ))
    assert(expectedUsers4.collect.toSet === graph4.vertices.collect.toSet)
    assert(expectedEdges4.collect.toSet === graph4.edges.collect.toSet)

    val graph5 = g.getSnapshot(LocalDate.parse("2012-01-01"))
    val expectedUsers5 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (3L, "Ron")
    ))
    assert(expectedUsers5.collect.toSet === graph5.vertices.collect.toSet)
    assert(graph5.edges.isEmpty())

    val graph6 = g.getSnapshot(LocalDate.parse("2014-01-01"))
    val expectedUsers6 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (2L, "Mike")
    ))
    val expectedEdges6 = ProgramContext.sc.parallelize(Array(
      Edge(1L, 2L, (2L, 22))
    ))
    assert(expectedUsers6.collect.toSet === graph6.vertices.collect.toSet)
    assert(expectedUsers6.collect.toSet === graph6.vertices.collect.toSet)

    val graph7 = g.getSnapshot(LocalDate.parse("2016-01-01"))
    val expectedUsers7 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (2L, "Mike")
    ))
    assert(expectedUsers7.collect.toSet === graph7.vertices.collect.toSet)
    assert(graph7.edges.isEmpty())

    val graph8 = g.getSnapshot(LocalDate.parse("2017-01-01"))
    val expectedUsers8 = ProgramContext.sc.parallelize(Array(
      (2L, "Mike")
    ))
    assert(expectedUsers8.collect.toSet === graph8.vertices.collect.toSet)
    assert(graph8.edges.isEmpty())
  }

  def testCCs(ge: TGraphNoSchema[String,Int]): Unit = {
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
    val edges: RDD[TEdge[Int]] = getTestEdges_Int_3().union(getTestEdges_Int_4a()).union(getTestEdges_Int_4b())

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

    val g = ge.fromRDDs(nodes, edges, "Default")

    val actualg = g.connectedComponents()
    assert(actualg.vertices.collect.toSet == expectedNodes.collect.toSet)
  }

  def testShortestPathsUndirected(ge: TGraphNoSchema[String,Int]): Unit = {
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

    val edges: RDD[TEdge[Int]] = getTestEdges_Int_3().union(getTestEdges_Int_4a())

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

    val g = ge.fromRDDs(nodes, edges, "Default")

    val actualg = g.shortestPaths(false, Seq(1L, 2L))

    assert(actualg.vertices.collect.toSet == expectedNodes.collect.toSet)
  }

  def testShortestPathsDirected(ge: TGraphNoSchema[String,Int]): Unit = {
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
    val edges: RDD[TEdge[Int]] = getTestEdges_Int_3().union(getTestEdges_Int_4a())

    val expectedNodes: RDD[(VertexId, (Interval, (String, Map[VertexId, Int])))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("John", new Long2IntOpenHashMap(Array(5L, 6L), Array(3, 2)).asInstanceOf[Map[VertexId,Int]]))),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Mike", new Long2IntOpenHashMap(Array(5L, 6L), Array(2, 1)).asInstanceOf[Map[VertexId,Int]]))),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Ron", new Long2IntOpenHashMap(Array(5L, 6L), Array(1, 2)).asInstanceOf[Map[VertexId,Int]]))),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Julia", new Long2IntOpenHashMap(Array(5L, 6L), Array(1, 2)).asInstanceOf[Map[VertexId,Int]]))),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Vera", new Long2IntOpenHashMap(Array(5L, 6L), Array(0, 1)).asInstanceOf[Map[VertexId,Int]]))),
      (6L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Halima", new Long2IntOpenHashMap(Array(6L), Array(0)).asInstanceOf[Map[VertexId,Int]]))),
      (7L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Sanjana", new Long2IntOpenHashMap().asInstanceOf[Map[VertexId, Int]]))),
      (8L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Lovro", new Long2IntOpenHashMap().asInstanceOf[Map[VertexId, Int]]))),

      //second representative graph
      (1L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("John", new Long2IntOpenHashMap(Array(5L), Array(1)).asInstanceOf[Map[VertexId,Int]]))),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Mike", new Long2IntOpenHashMap(Array(6L), Array(1)).asInstanceOf[Map[VertexId,Int]]))),
      (3L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Ron", new Long2IntOpenHashMap().asInstanceOf[Map[VertexId, Int]]))),
      (4L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Julia", new Long2IntOpenHashMap().asInstanceOf[Map[VertexId, Int]]))),
      (5L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Vera", new Long2IntOpenHashMap(Array(5L), Array(0)).asInstanceOf[Map[VertexId,Int]])))
    ))

    val g = ge.fromRDDs(nodes, edges, "Default")

    val actualg = g.shortestPaths(true, Seq(5L, 6L))
    assert(actualg.vertices.collect.toSet == expectedNodes.collect.toSet)
  }

  def testPageRankDirected(ge: TGraphNoSchema[String,Int]): Unit = {
    //PageRank for each representative graph was tested by creating graph in graphX and using spark's pagerank
    //The final g is sliced into the two representative graph to assert the values
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
    val edges: RDD[TEdge[Int]] = getTestEdges_Int_3().union(getTestEdges_Int_4a()).union(getTestEdges_Int_4b())

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
    val testEdges: RDD[Edge[(EdgeId,Int)]] = ProgramContext.sc.parallelize(Array(
      Edge(1L, 2L, (1L,42)),
      Edge(2L, 3L, (2L,42)),
      Edge(2L, 6L, (3L,42)),
      Edge(2L, 4L, (4L,42)),
      Edge(3L, 5L, (5L,42)),
      Edge(3L, 4L, (6L,42)),
      Edge(4L, 5L, (7L,42)),
      Edge(5L, 6L, (8L,42)),
      Edge(7L, 8L, (9L,42))
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
    val testEdges2: RDD[Edge[(EdgeId,Int)]] = ProgramContext.sc.parallelize(Array(
      Edge(1L, 3L, (10L,42)),
      Edge(1L, 5L, (11L,42)),
      Edge(3L, 7L, (12L,42)),
      Edge(5L, 7L, (13L,42)),
      Edge(2L, 4L, (4L,42)),
      Edge(2L, 6L, (3L,42)),
      Edge(4L, 8L, (14L,42)),
      Edge(6L, 8L, (15L,42))
    ))
    val graph2 = Graph(testNodes2, testEdges2, "Default")
    val pageRank2014_2018 = graph2.staticPageRank(10, 0.15)

    val pageRank2010_2014VerticesSorted = pageRank2010_2014.vertices.sortBy(_._1).collect()
    val pageRank2014_2018VerticesSorted = pageRank2014_2018.vertices.sortBy(_._1).collect()
    //End of Spark's pagerank

    //Pagerank using Portal api
    val g = ge.fromRDDs(nodes, edges, "Default")
    val actualg = g.pageRank(true, 0.001, 0.15, 10)
    val sliced2010_2014 = actualg.slice(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")))
    val sliced2014_2018 = actualg.slice(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")))
    val actualg2010_2014VerticesSorted = sliced2010_2014.vertices.sortBy(_._1).collect()
    val actualg2014_2018VerticesSorted = sliced2014_2018.vertices.sortBy(_._1).collect()
    //End of Portal pagerank

    //Assertion
    for (i <- 0 until pageRank2010_2014VerticesSorted.length) {
      val difference = pageRank2010_2014VerticesSorted(i)._2 - actualg2010_2014VerticesSorted(i)._2._2._2
      assert(Math.abs(difference) < 0.0000001)
    }

    for (i <- 0 until pageRank2014_2018VerticesSorted.length) {
      val difference = pageRank2014_2018VerticesSorted(i)._2 - actualg2014_2018VerticesSorted(i)._2._2._2
      assert(Math.abs(difference) < 0.0000001)
    }
  }

  def testPageRankUndirected(ge: TGraphNoSchema[String,Int]): Unit = {
    //PageRank for each representative graph was tested by creating graph in graphX and using spark's pagerank
    //Spark's pagerank only has directed pagerank so to test it, each edge is added both ways and spark's pagerank is computed
    //The final g is sliced into the two representative graph to assert the values
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
    val edges: RDD[TEdge[Int]] = getTestEdges_Int_3().union(getTestEdges_Int_4a()).union(getTestEdges_Int_4b())

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
    val g = ge.fromRDDs(nodes, edges, "Default")
    val actualg = g.pageRank(false, 0.001, 0.15, 10)
    val sliced2010_2014 = actualg.slice(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")))
    val sliced2014_2018 = actualg.slice(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")))
    val actualg2010_2014VerticesSorted = sliced2010_2014.vertices.sortBy(_._1).collect()
    val actualg2014_2018VerticesSorted = sliced2014_2018.vertices.sortBy(_._1).collect()
    //End of Portal pagerank

     //Assertion
    for (i <- 0 until pageRank2010_2014VerticesSorted.length) {
      val difference = pageRank2010_2014VerticesSorted(i)._2 - actualg2010_2014VerticesSorted(i)._2._2._2
      assert(Math.abs(difference) < 0.0000001)
    }

    for (i <- 0 until pageRank2014_2018VerticesSorted.length) {
      val difference = pageRank2014_2018VerticesSorted(i)._2 - actualg2014_2018VerticesSorted(i)._2._2._2
      assert(Math.abs(difference) < 0.0000001)
    }
  }

  def testAggregateMessages(ge: TGraphNoSchema[String,Int]): Unit = {
    val nodesAndEdges = AggregateMessagesTestUtil.getNodesAndEdges_v1

    var g = ge.fromRDDs(nodesAndEdges._1,nodesAndEdges._2,"Default")

    val result = g.aggregateMessages[Int](AggregateMessagesTestUtil.sendMsg_noPredicate, (a, b) => {a+b}, 0, TripletFields.None)

    AggregateMessagesTestUtil.assertions_noPredicate(result)
  }

  def testAggregateMessages2(ge: TGraphNoSchema[String,Int]): Unit = {
    val nodesAndEdges = AggregateMessagesTestUtil.getNodesAndEdges_v1

    var g = ge.fromRDDs(nodesAndEdges._1,nodesAndEdges._2,"Default")

    val result = g.aggregateMessages[Int](AggregateMessagesTestUtil.sendMsg_edgePredicate, (a, b) => {a+b}, 0, TripletFields.EdgeOnly)

    AggregateMessagesTestUtil.assertions_edgePredicate(result)
  }

  def testAggregateMessages3(ge: TGraphNoSchema[String,Int]): Unit = {
    val nodesAndEdges = AggregateMessagesTestUtil.getNodesAndEdges_v1

    var g = ge.fromRDDs(nodesAndEdges._1, nodesAndEdges._2, "Default")

    val result = g.aggregateMessages[Int](AggregateMessagesTestUtil.sendMsg_vertexPredicate, (a, b) => {
      a + b
    }, 0, TripletFields.All)

    AggregateMessagesTestUtil.assertions_vertexPredicate(result)
  }

  protected def getTestEdges_Int_1b(): RDD[TEdge[Int]] = {
    ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 4L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22),
      TEdge[Int](2L, 3L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 22),
      TEdge[Int](3L, 1L, 2L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22),
      TEdge[Int](4L, 5L, 7L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22),
      TEdge[Int](5L, 4L, 8L, Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 22),
      TEdge[Int](6L, 4L, 9L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)
    ))
  }

  protected def getTestEdges_Int_1a(): RDD[TEdge[Int]] = {
    ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 4L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42),
      TEdge[Int](2L, 3L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42),
      TEdge[Int](3L, 1L, 2L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22),
      TEdge[Int](4L, 5L, 7L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22),
      TEdge[Int](5L, 4L, 8L, Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42),
      TEdge[Int](6L, 4L, 9L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)
    ))
  }

  protected def getTestEdges_Int_2(): RDD[TEdge[Int]] = {
    ProgramContext.sc.parallelize(Array(
      TEdge[Int](7L, 4L, 6L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), 22),
      TEdge[Int](8L, 4L, 6L, Interval(LocalDate.parse("2012-06-01"), LocalDate.parse("2013-01-01")), 72)
    ))
  }

  protected def getTestEdges_Int_3(): RDD[TEdge[Int]] = {
    ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 2L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](2L, 2L, 3L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](3L, 2L, 6L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), 42),
      TEdge[Int](4L, 2L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), 42),
      TEdge[Int](5L, 3L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](6L, 3L, 4L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](7L, 4L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](8L, 5L, 6L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42),
      TEdge[Int](9L, 7L, 8L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 42)
    ))
  }

  protected def getTestEdges_Int_4a(): RDD[TEdge[Int]] = {
    ProgramContext.sc.parallelize(Array(
      TEdge[Int](10L, 1L, 3L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42),
      TEdge[Int](11L, 1L, 5L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42),
      TEdge[Int](12L, 3L, 7L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42),
      TEdge[Int](13L, 5L, 7L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)
    ))
  }

  protected def getTestEdges_Int_4b(): RDD[TEdge[Int]] = {
    ProgramContext.sc.parallelize(Array(
      TEdge[Int](14L, 4L, 8L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42),
      TEdge[Int](15L, 6L, 8L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), 42)
    ))
  }

  protected def getTestEdges_Bool_1(): RDD[TEdge[StructureOnlyAttr]] = {
    ProgramContext.sc.parallelize(Array(
      TEdge[StructureOnlyAttr](1L, 1L, 4L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true),
      TEdge[StructureOnlyAttr](2L, 3L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), true),
      TEdge[StructureOnlyAttr](3L, 1L, 2L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), true),
      TEdge[StructureOnlyAttr](4L, 5L, 7L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), true),
      TEdge[StructureOnlyAttr](5L, 4L, 8L, Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), true),
      TEdge[StructureOnlyAttr](6L, 4L, 9L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), true),
      TEdge[StructureOnlyAttr](7L, 4L, 6L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2015-01-01")), true),
      TEdge[StructureOnlyAttr](8L, 4L, 6L, Interval(LocalDate.parse("2012-06-01"), LocalDate.parse("2013-01-01")), true)
    ))
  }

  protected def getTestEdges_Bool_1a(): RDD[TEdge[StructureOnlyAttr]] = {
    ProgramContext.sc.parallelize(Array(
      TEdge[StructureOnlyAttr](1L, 1L, 4L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true),
      TEdge[StructureOnlyAttr](2L, 3L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), true),
      TEdge[StructureOnlyAttr](3L, 1L, 2L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), true),
      TEdge[StructureOnlyAttr](4L, 5L, 7L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), true),
      TEdge[StructureOnlyAttr](5L, 4L, 8L, Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), true),
      TEdge[StructureOnlyAttr](6L, 4L, 9L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), true),
      TEdge[StructureOnlyAttr](7L, 4L, 6L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), true)
    ))
  }

  def testTriangleCount(ge: TGraphNoSchema[String,Int]): Unit = {
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
    val edges: RDD[TEdge[Int]] = getTestEdges_Int_3().union(getTestEdges_Int_4a())

    val expectedNodes: RDD[(VertexId, (Interval, (String,Int)))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("John",0))),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Mike",1))),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Mike",0))),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Ron",2))),
      (3L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Ron",0))),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Julia",2))),
      (4L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Julia",0))),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Vera",1))),
      (5L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Vera",0))),
      (6L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Halima",0))),
      (7L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Sanjana",0))),
      (8L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Lovro",0)))
    ))

    val g = ge.fromRDDs(nodes, edges, "Default")

    val actualg = g.triangleCount()
    assert(actualg.vertices.collect.toSet == expectedNodes.collect.toSet)

  }

  def testCCoeff(ge: TGraphNoSchema[String,Int]): Unit = {
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
    val edges: RDD[TEdge[Int]] = getTestEdges_Int_3().union(getTestEdges_Int_4a())

    val expectedNodes: RDD[(VertexId, (Interval, (String,Double)))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("John",0.0))),
      (2L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Mike",1/12.0))),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Mike",0.0))),
      (3L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Ron",2/6.0))),
      (3L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Ron",0.0))),
      (4L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Julia",2/6.0))),
      (4L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Julia",0.0))),
      (5L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), ("Vera",1/6.0))),
      (5L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), ("Vera",0.0))),
      (6L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Halima",0.0))),
      (7L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Sanjana",0.0))),
      (8L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01")), ("Lovro",0.0)))
    ))

    val g = ge.fromRDDs(nodes, edges, "Default")

    val actualg = g.clusteringCoefficient()
    assert(actualg.vertices.collect.toSet == expectedNodes.collect.toSet)

  }

}
