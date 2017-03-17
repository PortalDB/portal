package edu.drexel.cs.dbgroup.temporalgraph.representations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph._

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
  protected override def beforeAll(): Unit = {
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

    assert(expectedg2.vertices.collect() === actualg2.vertices.collect())
    assert(expectedg2.edges.collect() === actualg2.edges.collect())
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
    val edges: RDD[TEdge[Int]] = getTestEdges_Int_1b()

}
