package edu.drexel.cs.dbgroup.temporalgraph

import java.time.LocalDate

import org.apache.spark.graphx.{EdgeTriplet, VertexId}
import org.apache.spark.rdd.RDD

/**
  * Created by mtg5014 on 1/26/2017.
  */
class AggregateMessagesTestUtil {

}

object AggregateMessagesTestUtil {

  val FRIEND = "friend"
  val ENEMY = "enemy"

  val tenToEighteen = Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2018-01-01"))
  val tenToFourteen = Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2014-01-01"))
  val fourteenToEighteen = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2018-01-01"))

  val sendMsg_noPredicate = (triplet: TEdgeTriplet[String,String]) => {
    Iterator((triplet.dstId,1),(triplet.srcId,1))
  }

  val sendMsg_edgePredicate = (triplet: TEdgeTriplet[String, String]) => {
    if(triplet.attr.contentEquals("friend")) {
      Iterator((triplet.dstId,1),(triplet.srcId,1))
    }
    else {
      Iterator.empty
    }
  }

  val sendMsg_vertexPredicate = (triplet: TEdgeTriplet[String, String]) => {
    if(triplet.srcAttr.contentEquals("John")){
      Iterator((triplet.srcId,1))
    }
    else if(triplet.dstAttr.contentEquals("John")){
      Iterator((triplet.dstId,1))
    }
    else{
      Iterator.empty
    }
  }

  def getNodesAndEdges_v1(): (RDD[(VertexId, (Interval, String))],RDD[TEdge[String]]) = {
    //edge attributes for later - for each node we have a count of friends - we ignore enemy edges


    val nodes: RDD[(VertexId, (Interval, String))] = ProgramContext.sc.parallelize(Array(
      (1l, (tenToEighteen,"John")),
      (2l, (tenToEighteen,"Mike")),
      (3l, (tenToEighteen,"Bob")),
      (4l, (tenToEighteen,"James")),
      (5l, (tenToEighteen,"Matt")),
      (6l, (tenToEighteen,"Bill"))
    ))

    val edges: RDD[TEdge[String]] = ProgramContext.sc.parallelize(Array(
      //john, mike, and bob are always friends
      TEdge[String](1L,1l,2l, tenToEighteen, FRIEND),
      TEdge[String](2L,1l,3l, tenToEighteen, FRIEND),
      TEdge[String](3L,2l,3l, tenToEighteen, FRIEND),
      //james, matt, and bill are always enemies
      TEdge[String](4L,4l,5l, tenToEighteen, ENEMY),
      TEdge[String](5L,4l,6l, tenToEighteen, ENEMY),
      TEdge[String](6L,5l,6l, tenToEighteen, ENEMY),
      //john is friends with james, matt, and bill through 2014, but enemies after
      TEdge[String](7L,1l,4l, tenToFourteen, FRIEND),
      TEdge[String](8L,1l,5l, tenToFourteen, FRIEND),
      TEdge[String](9L,1l,6l, tenToFourteen, FRIEND),
      TEdge[String](10L,1l,4l, fourteenToEighteen, ENEMY),
      TEdge[String](11L,1l,5l, fourteenToEighteen, ENEMY),
      TEdge[String](12L,1l,6l, fourteenToEighteen, ENEMY),
      //mike is friends with james, matt, and bill, but only through 2014
      TEdge[String](13L,2l,4l, tenToFourteen, FRIEND),
      TEdge[String](14L,2l,5l, tenToFourteen, FRIEND),
      TEdge[String](15L,2l,6l, tenToFourteen, FRIEND),
      //bob is enemies with james, matt, and bill after 2014
      TEdge[String](16L,3l,4l, fourteenToEighteen, ENEMY),
      TEdge[String](17L,4l,5l, fourteenToEighteen, ENEMY),
      TEdge[String](18L,5l,6l, fourteenToEighteen, ENEMY)
    ))
    (nodes,edges)
  }

  def assertSingle(node: (VertexId, (Interval, (String, Int))),
                   id: VertexId,
                   interval: Interval,
                   name: String,
                   count: Int) : Unit =
  {
    assert(node._1 == id)
    assert(node._2._1.compare(interval) == 0)
    assert(node._2._2._1.contentEquals(name))
    assert(node._2._2._2 == count)
  }

  def assertions_noPredicate(result: TGraphNoSchema[(String,Int),String]) : Unit = {

    var resultList = result.vertices.collect().sortBy(v => (v._1, v._2._1)).toList

    println(resultList
      .sortBy(v => (v._1, v._2._1))
      .mkString("\r\n"))

    assert(resultList.size == 10)

    for(i <- 0 until resultList.size) {
      var node = resultList(i)
      i match {
        case 0 => assertSingle(node,1,tenToEighteen,"John",5)
        case 1 => assertSingle(node,2,tenToFourteen,"Mike",5)
        case 2 => assertSingle(node,2,fourteenToEighteen,"Mike",2)
        case 3 => assertSingle(node,3,tenToFourteen,"Bob",2)
        case 4 => assertSingle(node,3,fourteenToEighteen,"Bob",3)
        case 5 => assertSingle(node,4,tenToFourteen,"James",4)
        case 6 => assertSingle(node,4,fourteenToEighteen,"James",5)
        case 7 => assertSingle(node,5,tenToFourteen,"Matt",4)
        case 8 => assertSingle(node,5,fourteenToEighteen,"Matt",5)
        case 9 => assertSingle(node,6,tenToEighteen,"Bill",4)
      }
    }
  }

  def assertions_edgePredicate(result: TGraphNoSchema[(String,Int),String]) : Unit = {

    var resultList = result.vertices.collect().sortBy(v => (v._1, v._2._1)).toList

    println(resultList
      .sortBy(v => (v._1, v._2._1))
      .mkString("\r\n"))

    assert(resultList.size == 11)

    for(i <- 0 until resultList.size) {
      var node = resultList(i)
      i match {
        case 0 => assertSingle(node,1,tenToFourteen,"John",5)
        case 1 => assertSingle(node,1,fourteenToEighteen,"John",2)
        case 2 => assertSingle(node,2,tenToFourteen,"Mike",5)
        case 3 => assertSingle(node,2,fourteenToEighteen,"Mike",2)
        case 4 => assertSingle(node,3,tenToEighteen,"Bob",2)
        case 5 => assertSingle(node,4,tenToFourteen,"James",2)
        case 6 => assertSingle(node,4,fourteenToEighteen,"James",0)
        case 7 => assertSingle(node,5,tenToFourteen,"Matt",2)
        case 8 => assertSingle(node,5,fourteenToEighteen,"Matt",0)
        case 9 => assertSingle(node,6,tenToFourteen,"Bill",2)
        case 10 => assertSingle(node,6,fourteenToEighteen,"Bill",0)
      }
    }
  }

  def assertions_vertexPredicate(result: TGraphNoSchema[(String,Int),String]) : Unit = {
    var resultList = result.vertices.collect().sortBy(v => (v._1, v._2._1)).toList

    println(resultList
      .sortBy(v => (v._1, v._2._1))
      .mkString("\r\n"))

    assert(resultList.size == 6)

    for(i <- 0 until resultList.size) {
      var node = resultList(i)
      i match {
        case 0 => assertSingle(node,1,tenToEighteen,"John",5)
        case 1 => assertSingle(node,2,tenToEighteen,"Mike",0)
        case 2 => assertSingle(node,3,tenToEighteen,"Bob",0)
        case 3 => assertSingle(node,4,tenToEighteen,"James",0)
        case 4 => assertSingle(node,5,tenToEighteen,"Matt",0)
        case 5 => assertSingle(node,6,tenToEighteen,"Bill",0)
      }
    }
  }
}
