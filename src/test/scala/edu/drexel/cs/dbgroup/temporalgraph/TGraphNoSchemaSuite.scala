package edu.drexel.cs.dbgroup.temporalgraph

import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph.representations.SnapshotGraphParallel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.parallel.ParSeq

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

  test("verticesAggregated and edgesAggregated functions"){
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

    assert(actualSGP.verticesAggregated.collect.toSet === expectedVertices.collect.toSet)
    assert(actualSGP.edgesAggregated.collect.toSet === expectedEdges.collect.toSet)
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
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22)),
    //partially outside, completely inside
      ((3L, 5L), (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
    //completely outside
      ((5L, 7L), (Interval(LocalDate.parse("2008-01-01"), LocalDate.parse("2009-01-01")), 22)),
    //inside in the boundries
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 22)),
    //partially outside, partially inside
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2016-01-01")), 22))
    ))
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

    val actualEdges = TGraphNoSchema.constrainEdges(users, edges)

    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22)),
      ((3L, 5L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01")), 22)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22)),
      ((4L, 8L), (Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 22)),
      ((4L, 9L), (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22))
    ))

    assert(actualEdges.collect.toSet === expectedEdges.collect.toSet)
  }

  test("from RDD") {
    //Checks if the fromRDD function creates the correct graphs. Graphs variable is protected so to get the graphs, we use getSnapshot function
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John")),
      (2L, (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike")),
      (3L, (Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron"))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 3L), (Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2012-01-01")), 42)),
      ((1L, 2L), (Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22))
    ))
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    //should be empty
    val graph1 = sgp.getSnapshot(LocalDate.parse("2008-12-01"))
    val graph2 = sgp.getSnapshot(LocalDate.parse("2018-01-01"))

    assert(graph1.vertices.isEmpty())
    assert(graph1.edges.isEmpty())

    assert(graph2.vertices.isEmpty())
    assert(graph2.edges.isEmpty())

    //not empty
    val graph3 = sgp.getSnapshot(LocalDate.parse("2009-01-01"))
    val expectedUsers3 = ProgramContext.sc.parallelize(Array(
      (3L, "Ron")
    ))
    assert(expectedUsers3.collect.toSet === graph3.vertices.collect.toSet)
    assert(graph3.edges.isEmpty())

    val graph4 = sgp.getSnapshot(LocalDate.parse("2010-01-01"))
    val expectedUsers4 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (3L, "Ron")
    ))
    val expectedEdges4 = ProgramContext.sc.parallelize(Array(
      Edge(1L, 3L, 42)
    ))
    assert(expectedUsers4.collect.toSet === graph4.vertices.collect.toSet)
    assert(expectedEdges4.collect.toSet === graph4.edges.collect.toSet)

    val graph5 = sgp.getSnapshot(LocalDate.parse("2012-01-01"))
    val expectedUsers5 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (3L, "Ron")
    ))
    assert(expectedUsers5.collect.toSet === graph5.vertices.collect.toSet)
    assert(graph5.edges.isEmpty())

    val graph6 = sgp.getSnapshot(LocalDate.parse("2014-01-01"))
    val expectedUsers6 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (2L, "Mike")
    ))
    val expectedEdges6 = ProgramContext.sc.parallelize(Array(
      Edge(1L, 2L, 22)
    ))
    assert(expectedUsers6.collect.toSet === graph6.vertices.collect.toSet)
    assert(expectedUsers6.collect.toSet === graph6.vertices.collect.toSet)

    val graph7 = sgp.getSnapshot(LocalDate.parse("2016-01-01"))
    val expectedUsers7 = ProgramContext.sc.parallelize(Array(
      (1L, "John"),
      (2L, "Mike")
    ))
    assert(expectedUsers7.collect.toSet === graph7.vertices.collect.toSet)
    assert(graph7.edges.isEmpty())

    val graph8 = sgp.getSnapshot(LocalDate.parse("2017-01-01"))
    val expectedUsers8 = ProgramContext.sc.parallelize(Array(
      (2L, "Mike")
    ))
    assert(expectedUsers8.collect.toSet === graph8.vertices.collect.toSet)
    assert(graph8.edges.isEmpty())
  }

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
    var edges2 : EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.parallelize(Array(
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
    var edges3 : EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.parallelize(Array(
      Edge(7L, 8L, 22)
    )))
    val graph3 = Graph(users3, edges3, "Default", edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)

    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-01-01"))

    intervals = intervals :+ testInterval1 :+ testInterval2 :+ testInterval3
    graphs = graphs :+ graph1 :+ graph2 :+ graph3

    val actualSgp = SnapshotGraphParallel.fromGraphs(intervals, graphs, "Default", StorageLevel.MEMORY_ONLY_SER)

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

    assert(users.collect().toSet === actualSgp.vertices.collect().toSet)
    assert(edges.collect().toSet === actualSgp.edges.collect().toSet)
  }

}
