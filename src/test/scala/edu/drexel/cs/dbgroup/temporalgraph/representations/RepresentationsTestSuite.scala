package edu.drexel.cs.dbgroup.temporalgraph.representations

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
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

  protected def testSlice[T:TypeTag:ClassTag]:Unit = {
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
    val g = reflect[String,Int,T](users, edges, "Default")

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
    val expectedg = reflect[String,Int,T](expectedUsers, expectedEdges, "Default")
    //behavior is different for materialized and unmaterialized graph
    //although the result should be the same
    g.materialize
    var actualg = g.slice(sliceInterval)

    assert(expectedg.vertices.collect() === actualg.vertices.collect())
    assert(expectedg.edges.collect() === actualg.edges.collect())
    assert(expectedg.getTemporalSequence.collect === actualg.getTemporalSequence.collect)
    info("regular cases passed")

  }

  //FIXME: this is not yet working
  private def reflect[VD: ClassTag, ED: ClassTag, T:TypeTag:ClassTag](verts: RDD[(VertexId, (Interval, VD))], edges: RDD[TEdge[ED]], deflt: VD): TGraphNoSchema[VD,ED] = {
    val ru = scala.reflect.runtime.universe
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    val runtimeClass = classTag[T].runtimeClass
    val rootMirror = ru.runtimeMirror(runtimeClass.getClassLoader)
    val classSymbol = rootMirror.classSymbol(runtimeClass)
    // get the companion here
    val members = classSymbol.companion.typeSignature.members
    val fromRDDsSymbol = members.find(m => m.name == ru.TermName("fromRDDs")).get.asMethod

    val instanceMirror = rm.reflect(classTag[T])
    //val fromRDDsSymbol = ru.typeOf[T].decl(ru.TermName("fromRDDs")).asMethod
    val fromRDDs = instanceMirror.reflectMethod(fromRDDsSymbol)
    fromRDDs(verts, edges, deflt).asInstanceOf[TGraphNoSchema[VD,ED]]
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
