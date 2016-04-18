package edu.drexel.cs.dbgroup.temporalgraph

import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph.representations.SnapshotGraphParallel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfter, FunSuite}

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

    //During collapsing, the rdd dont save the order so they are sorted for the test.
    assert(expectedCoalesce.collect().sortBy(_._1) === actualCoalesce.collect().sortBy(_._1))
    assert(expectedCoalesce.collect().sortBy(_._1) === actualCoalesce.collect().sortBy(_._1))
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


    //During collapsing, the rdd dont save the order so they are sorted for the test.
    assert(expectedCoalesce.collect().sortBy(_._1) === actualCoalesce.collect().sortBy(_._1))
    assert(expectedCoalesce.collect().sortBy(_._1) === actualCoalesce.collect().sortBy(_._1))
  }

  test("vertices and edges functions"){
    val vertices: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2022-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron")),
      (2L, (Interval(LocalDate.parse("2018-02-01"), LocalDate.parse("2020-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2019-01-01"), LocalDate.parse("2022-01-01")), "Ron")),
      (4L, (Interval(LocalDate.parse("2006-01-01"), LocalDate.parse("2017-01-01")), "Julia")),
      (4L, (Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2019-07-14")), "Vera"))
    ))

    val expectedVertices: RDD[(VertexId, Map[Interval, String])] = ProgramContext.sc.parallelize(Array(
      (1L, Map(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2022-01-01")) -> "John")),
      (2L, Map(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")) -> "Mike", Interval(LocalDate.parse("2018-02-01"), LocalDate.parse("2020-01-01")) ->"Mike")),
      (3L, Map(Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")) -> "Ron", Interval(LocalDate.parse("2019-01-01"), LocalDate.parse("2022-01-01")) -> "Ron")),
      (4L, Map(Interval(LocalDate.parse("2006-01-01"), LocalDate.parse("2017-01-01")) -> "Julia", Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2019-07-14")) -> "Vera" ))
    ))

    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), 42)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((5L, 7L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22)),
      ((1L, 4L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), 56)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)),
      ((1L, 4L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 12)),
      ((3L, 5L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), 42))
    ))

    val expectedEdges: RDD[((VertexId,VertexId),Map[Interval, Int])] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), Map(Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")) -> 42, Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")) -> 12, Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")) -> 56)),
      ((3L, 5L), Map(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")) -> 42, Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")) -> 42)),
      ((1L, 2L), Map(Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")) -> 22)),
      ((5L, 7L), Map(Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")) -> 22)),
      ((4L, 8L), Map(Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")) -> 42))
    ))

    val actualSGP = SnapshotGraphParallel.fromRDDs(vertices, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

    println("actual vertices")
    actualSGP.vertices.collect().foreach(println)
    println("actual edges")
    actualSGP.edges.collect().foreach(println)

    //The rdds dont save the order so they are sorted for the test.
    assert(actualSGP.vertices.sortBy(_._1).collect() === expectedVertices.sortBy(_._1).collect())
    assert(actualSGP.edges.sortBy(_._1).collect().sortBy(_._1) === expectedEdges.sortBy(_._1).collect())
  }

  test("constrainEdges function"){
    //The method is protected, also it should have been tested with slice and select functions
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
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22)),
    //partially outside, completely inside
      ((3L, 5L), (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
    //completely outside
      ((5L, 7L), (Interval(LocalDate.parse("2008-01-01"), LocalDate.parse("2009-01-01")), 22)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 22)),
    //partially outside, partially inside
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2016-01-01")), 22))
    ))
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )
  }


}
