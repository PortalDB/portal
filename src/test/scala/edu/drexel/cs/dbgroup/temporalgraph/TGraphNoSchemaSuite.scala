package edu.drexel.cs.dbgroup.temporalgraph

import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph.representations.SnapshotGraphParallel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}
import src.main.scala.edu.drexel.cs.dbgroup.temporalgraph.TEdge

class TGraphNoSchemaSuite extends FunSuite with BeforeAndAfter{

  before {
    if(ProgramContext.sc == null){
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME")).setMaster("local[2]")
      val sc = new SparkContext(conf)
      ProgramContext.setContext(sc)
    }
  }

  test("coalesce function"){
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2018-07-01"), LocalDate.parse("2022-01-01")), "John")),
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-07-14"), LocalDate.parse("2017-01-01")), "Julia")),
      (1L, (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-07-01")), "John")),
      (2L, (Interval(LocalDate.parse("2018-02-01"), LocalDate.parse("2020-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2019-01-01"), LocalDate.parse("2022-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2006-01-01"), LocalDate.parse("2012-07-14")), "Julia")),
      (4L, (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2019-07-14")), "Vera"))

    ))
    val actualCoalesce = TGraphNoSchema.coalesce(users)

    val expectedCoalesce: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2022-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (2L, (Interval(LocalDate.parse("2018-02-01"), LocalDate.parse("2020-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2019-01-01"), LocalDate.parse("2022-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2006-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (4L, (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2019-07-14")), "Vera"))
    ))

    assert(expectedCoalesce.collect.toSet === actualCoalesce.collect.toSet)
    assert(expectedCoalesce.collect.toSet === actualCoalesce.collect.toSet)
  }

  test("coalesce Structure function"){
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2018-07-01"), LocalDate.parse("2022-01-01")), "John")),
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2012-07-14"), LocalDate.parse("2017-01-01")), "Julia")),
      (1L, (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-07-01")), "John")),
      (2L, (Interval(LocalDate.parse("2018-02-01"), LocalDate.parse("2020-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2019-01-01"), LocalDate.parse("2022-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2006-01-01"), LocalDate.parse("2012-07-14")), "Julia")),
      (4L, (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2019-07-14")), "Vera"))

    ))
    val actualCoalesce = TGraphNoSchema.coalesceStructure(users)

    val expectedCoalesce: RDD[(VertexId, Interval)] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2022-01-01")))),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")))),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")))),
      (2L, (Interval(LocalDate.parse("2018-02-01"), LocalDate.parse("2020-01-01")))),
      (3L, (Interval(LocalDate.parse("2019-01-01"), LocalDate.parse("2022-01-01")))),
      (4L, (Interval(LocalDate.parse("2006-01-01"), LocalDate.parse("2019-07-14"))))
    ))

    assert(expectedCoalesce.collect.toSet === actualCoalesce.collect.toSet)
    assert(expectedCoalesce.collect.toSet === actualCoalesce.collect.toSet)
  }

  test("constrainEdges function"){
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
    val edges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 4L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22),
      //partially outside, completely inside
      TEdge[Int](2L, 3L, 5L, Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), 22),
      TEdge[Int](3L, 1L, 2L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22),
      //completely outside
      TEdge[Int](4L, 5L, 7L, Interval(LocalDate.parse("2008-01-01"), LocalDate.parse("2009-01-01")), 22),
      //inside in the boundries
      TEdge[Int](5L, 4L, 8L, Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 22),
      //partially outside, partially inside
      TEdge[Int](6L, 4L, 9L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2016-01-01")), 22)
    ))
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

    val actualEdges = TGraphNoSchema.constrainEdges(users, edges)

    val expectedEdges: RDD[TEdge[Int]] = ProgramContext.sc.parallelize(Array(
      TEdge[Int](1L, 1L, 4L, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22),
      TEdge[Int](2L, 3L, 5L, Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 22),
      TEdge[Int](3L, 1L, 2L, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22),
      TEdge[Int](5L, 4L, 8L, Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 22),
      TEdge[Int](6L, 4L, 9L, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)
    ))

    assert(actualEdges.collect.toSet === expectedEdges.collect.toSet)
  }


}
