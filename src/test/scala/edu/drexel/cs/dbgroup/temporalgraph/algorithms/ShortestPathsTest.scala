package edu.drexel.cs.dbgroup.temporalgraph.algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import java.time.LocalDate
import edu.drexel.cs.dbgroup.temporalgraph.{Interval, ProgramContext}
import edu.drexel.cs.dbgroup.temporalgraph.representations.{SnapshotGraphParallel}

import scala.collection.parallel.ParSeq
import org.apache.spark.graphx.{Edge, EdgeRDD, VertexId, Graph}

class ShortestPathsTest extends FunSuite with BeforeAndAfter{

  val programContext = ProgramContext

  before {
    if(programContext.sc == null){
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      var conf = new SparkConf().setAppName("TemporalGraph Test Project")
        .setSparkHome(System.getenv("SPARK_HOME"))
        .setMaster("local[2]")

      val sc = new SparkContext(conf)
      programContext.setContext(sc)
    }
  }

  test("SnapshotGraphParallel ShortestPaths test"){
    var gps: ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]]()
    var interv: Seq[Interval] = Seq[Interval]()

    val tmp1 = createTestGraph1()
    val intvs1 = tmp1._1
    interv = interv :+ intvs1
    gps = gps :+ tmp1._2

    val tmp2 = createTestGraph2()
    val intvs2 = tmp2._1
    interv = interv :+ intvs2
    gps = gps :+ tmp2._2

    val testGraph = new SnapshotGraphParallel(interv, gps)
    var landmarks = Seq(1L, 4L, 6L)

    val testInterv = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01"))
    var sel = testGraph.select(testInterv)
    assert(sel.vertices.count() === 10)
    assert(sel.edges.count() === 12)

    val sp = testGraph.shortestPaths(landmarks)
    Console.println(sp.vertices.collect.mkString("\n"))

    val expectedGraphs = Map[Long, Int](
      (1L -> 2), (2L -> 1), (3L -> 2), (4L -> 1), (5L -> 1),
      (6L -> 2), (7L -> 2), (8L -> 1), (9L -> 1), (10L -> 1)
    )

    //val expectedPaths = Map[Long, Array[(Int, Int)]](
    //  (1L -> [()])
    //)

    val tmp = sp.vertices.collect()
    tmp.foreach{
      case(k,v) => {
        assert(v.size === expectedGraphs(k))

        if(k == 3){
          val tmp = v(intvs1)
          assert(tmp.size === 1)
          assert(tmp.contains(6L))
          assert(tmp.get(6L).get === 1)
        }

        if(k == 9){
          val tmp = v(intvs2)
          assert(tmp.isEmpty)
        }
        null
      }
    }
  }

  def createTestGraph1(): (Interval, Graph[String, Int]) = {
    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01"))

    var users: RDD[(VertexId, String)] = ProgramContext.sc.parallelize(Array[(VertexId, String)](
      (1L, "Hans Röttger"),
      (2L, "Sarah Jane Delany"),
      (3L, "Keith Hoyes"),
      (4L, "Robert Neßelrath"),
      (5L, "Marshall R. Mayberry"),
      (6L, "Norbert Pfleger"),
      (7L, "Mary F. Fernandez")
    ))

    var edges: EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.parallelize(Array(
      Edge(1L, 4L, 2),
      Edge(1L, 5L, 3),
      Edge(2L, 4L, 0),
      Edge(4L, 7L, 2),
      Edge(3L, 6L, 0),
      Edge(7L, 2L, 0),
      Edge(2L, 1L, 1)
    )))

    val graph = Graph(users, edges, "Default", edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)
    (testInterval1 -> graph)
  }

  def createTestGraph2(): (Interval, Graph[String, Int]) = {
    val testInterval2 = Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01"))

    var users: RDD[(VertexId, String)] = ProgramContext.sc.parallelize(Array[(VertexId, String)](
      (1L, "Hans Röttger"),
      (3L, "Keith Hoyes"),
      (6L, "Norbert Pfleger"),
      (7L, "Mary F. Fernandez"),
      (8L, "Alon Y. Levy"),
      (9L, "Matthew W. Crocker"),
      (10L, "Pádraig Cunningham")
    ))

    var edges: EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.parallelize(Array(
      Edge(1L, 3L, 12),
      Edge(10L, 6L, 102),
      Edge(6L, 1L, 5),
      Edge(8L, 7L, 12),
      Edge(6L, 7L, 5)
    )))

    val graph = Graph(users, edges, "Default", edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)
    (testInterval2 -> graph)
  }
}
