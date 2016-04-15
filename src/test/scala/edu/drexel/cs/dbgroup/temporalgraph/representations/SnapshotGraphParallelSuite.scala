package edu.drexel.cs.dbgroup.temporalgraph.representations

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

import scala.collection.parallel.ParSeq
import org.apache.spark.graphx.{Edge, EdgeRDD, VertexId, Graph}

class SnapshotGraphParallelSuite  extends FunSuite with BeforeAndAfter {

  before {
    if(ProgramContext.sc == null){
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME")).setMaster("local[2]")
      val sc = new SparkContext(conf)
      ProgramContext.setContext(sc)
      println(" ") //the first line starts from between
    }
  }

  test("slice function"){
    //Regular cases
    var sliceInterval = (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")))
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John"))),
      (2L, ((Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike"))),
      (3L, ((Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron"))),
      (4L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia"))),
      (5L, ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera"))),
      (6L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))),
      (7L, ((Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana"))),
      (8L, ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro"))),
      (9L, ((Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke")))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22))),
      ((3L, 5L), ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 22))),
      ((1L, 2L), ((Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22))),
      ((5L, 7L), ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22))),
      ((4L, 8L), ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 22))),
      ((4L, 9L), ((Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)))
    ))
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "John"))),
      (2L, ((Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), "Mike"))),
      (3L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2014-01-01")), "Ron"))),
      (4L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Julia"))),
      (5L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Vera"))),
      (6L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))),
      (9L, ((Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke")))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22))),
      ((3L, 5L), ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")), 22))),
      ((1L, 2L), ((Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2015-01-01")), 22))),
      ((4L, 9L), ((Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)))
    ))
    val expectedSgp = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER )

    var actualSGP = sgp.slice(sliceInterval)

    assert(expectedSgp.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSgp.edges.collect() === actualSGP.edges.collect())
    info("regular cases passed")

    //When interval is completely outside the graph
    val sliceInterval2 = (Interval(LocalDate.parse("2001-01-01"), LocalDate.parse("2003-01-01")))
    val actualSGP2 = sgp.slice(sliceInterval2)
    assert(actualSGP2.vertices.collect() === SnapshotGraphParallel.emptyGraph().vertices.collect())
    assert(actualSGP2.edges.collect() === SnapshotGraphParallel.emptyGraph().edges.collect())
    info("interval completely outside the graph passed")

    //When the graph is empty
    val actualSGP3 = SnapshotGraphParallel.emptyGraph().slice(sliceInterval2)
    assert(actualSGP3.vertices.collect() === SnapshotGraphParallel.emptyGraph().vertices.collect())
    assert(actualSGP3.edges.collect() === SnapshotGraphParallel.emptyGraph().edges.collect())
    info("empty graph passed")
  }

  test("temporal select function"){
    //Regular cases
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John"))),
      (2L, ((Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike"))),
      (3L, ((Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron"))),
      (4L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia"))),
      (5L, ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera"))),
      (6L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))),
      (7L, ((Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana"))),
      (8L, ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro"))),
      (9L, ((Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke")))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22))),
      ((3L, 5L), ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 22))),
      ((1L, 2L), ((Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22))),
      ((5L, 7L), ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22))),
      ((4L, 8L), ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 22))),
      ((4L, 9L), ((Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)))
    ))
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (6L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima")))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 22)))
    ))
    val expectedSgp = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER )
    var selectFunction = (x:Interval) => x.equals((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01"))))
    var actualSGP = sgp.select(selectFunction, selectFunction)

    assert(expectedSgp.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSgp.edges.collect() === actualSGP.edges.collect())
    info("regular cases passed")

    //When interval is completely outside the graph
    selectFunction = (x:Interval) => x.equals((Interval(LocalDate.parse("2001-01-01"), LocalDate.parse("2003-01-01"))))
    val actualSGP2 = sgp.select(selectFunction, selectFunction)
    assert(actualSGP2.vertices.collect() === SnapshotGraphParallel.emptyGraph().vertices.collect())
    assert(actualSGP2.edges.collect() === SnapshotGraphParallel.emptyGraph().edges.collect())
    info("interval completely outside the graph passed")

    //When the graph is empty
    val actualSGP3 = SnapshotGraphParallel.emptyGraph().select(selectFunction, selectFunction)
    assert(actualSGP3.vertices.collect() === SnapshotGraphParallel.emptyGraph().vertices.collect())
    assert(actualSGP3.edges.collect() === SnapshotGraphParallel.emptyGraph().edges.collect())
    info("empty graph passed")
  }

  test("structural select function - epred"){
    //Regular cases
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John"))),
      (2L, ((Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike"))),
      (3L, ((Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron"))),
      (4L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia"))),
      (5L, ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera"))),
      (6L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))),
      (7L, ((Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana"))),
      (8L, ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro"))),
      (9L, ((Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke")))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42))),
      ((3L, 5L), ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42))),
      ((1L, 2L), ((Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22))),
      ((5L, 7L), ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22))),
      ((4L, 8L), ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42))),
      ((4L, 9L), ((Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)))
    ))
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John"))),
      (2L, ((Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike"))),
      (3L, ((Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron"))),
      (4L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia"))),
      (5L, ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera"))),
      (6L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))),
      (7L, ((Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana"))),
      (8L, ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro"))),
      (9L, ((Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke")))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((3L, 5L), ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42))),
      ((4L, 8L), ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)))
    ))
    val expectedSgp = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER )
    var actualSGP = sgp.select(epred = (ids, attrs) => ids._1 > 2 && attrs._2 == 42)

    assert(expectedSgp.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSgp.edges.collect() === actualSGP.edges.collect())
    info("regular cases passed")

//    //When the graph is empty
//    val actualSGP3 = SnapshotGraphParallel.emptyGraph().select(epred = (ids, attrs) => ids._1 > 2 && attrs._2 == 42)
//    assert(actualSGP3.vertices.collect() === SnapshotGraphParallel.emptyGraph().vertices.collect())
//    assert(actualSGP3.edges.collect() === SnapshotGraphParallel.emptyGraph().edges.collect())
//    info("empty graph passed")
  }

  test("structural select function - vpred"){
    //Regular cases
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John"))),
      (2L, ((Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike"))),
      (3L, ((Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron"))),
      (4L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia"))),
      (5L, ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera"))),
      (6L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))),
      (7L, ((Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana"))),
      (8L, ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro"))),
      (9L, ((Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke")))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42))),
      ((3L, 5L), ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42))),
      ((1L, 2L), ((Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22))),
      ((5L, 7L), ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22))),
      ((4L, 8L), ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42))),
      ((4L, 9L), ((Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)))
    ))
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER )

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (4L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia"))),
      (5L, ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera"))),
      (6L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))),
      (7L, ((Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana"))),
      (8L, ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((5L, 7L), ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22))),
      ((4L, 8L), ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)))
    ))
    val expectedSgp = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER )
    var actualSGP = sgp.select(vpred = (id, attrs) => id > 3 && attrs._2 != "Ke")

    assert(expectedSgp.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSgp.edges.collect() === actualSGP.edges.collect())
    info("regular cases passed")
  }

  test("structural select function - vpred and epred") {
    //Regular cases
    val users: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1L, ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2017-01-01")), "John"))),
      (2L, ((Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01")), "Mike"))),
      (3L, ((Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2014-01-01")), "Ron"))),
      (4L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia"))),
      (5L, ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera"))),
      (6L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))),
      (7L, ((Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana"))),
      (8L, ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro"))),
      (9L, ((Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), "Ke")))
    ))
    val edges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((1L, 4L), ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), 42))),
      ((3L, 5L), ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2013-01-01")), 42))),
      ((1L, 2L), ((Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01")), 22))),
      ((5L, 7L), ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2011-01-01")), 22))),
      ((4L, 8L), ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42))),
      ((4L, 9L), ((Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")), 22)))
    ))
    val sgp = SnapshotGraphParallel.fromRDDs(users, edges, "Default", StorageLevel.MEMORY_ONLY_SER)

    val expectedUsers: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (4L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2017-01-01")), "Julia"))),
      (5L, ((Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2015-01-01")), "Vera"))),
      (6L, ((Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")), "Halima"))),
      (7L, ((Interval(LocalDate.parse("2009-01-01"), LocalDate.parse("2011-01-01")), "Sanjana"))),
      (8L, ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), "Lovro")))
    ))
    val expectedEdges: RDD[((VertexId, VertexId), (Interval, Int))] = ProgramContext.sc.parallelize(Array(
      ((4L, 8L), ((Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01")), 42)))
    ))
    val expectedSgp = SnapshotGraphParallel.fromRDDs(expectedUsers, expectedEdges, "Default", StorageLevel.MEMORY_ONLY_SER)
    var actualSGP = sgp.select(vpred = (id, attrs) => id > 3 && attrs._2 != "Ke", epred = (ids, attrs) => ids._1 > 2 && attrs._2 == 42)

    assert(expectedSgp.vertices.collect() === actualSGP.vertices.collect())
    assert(expectedSgp.edges.collect() === actualSGP.edges.collect())
    info("regular cases passed")
  }
}
