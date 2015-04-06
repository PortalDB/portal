package edu.drexel.cs.dbgroup.graphxt

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.collection.immutable.Map
import scala.collection.breakOut

//pageRank implementation using Pregel interface, but for an undirected graph
object UndirectedPageRank {

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15, maxIter: Int = Int.MaxValue): Graph[Double, Double] =
  {
    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/degree and each vertex with attribute 1.0.
    val pagerankGraph: Graph[(Double, Double), (Double,Double)] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.degrees) {
        (vid, vdata, deg) => deg.getOrElse(0)
      }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => (1.0 / e.srcAttr, 1.0 / e.dstAttr) )
      // Set the vertex attributes to (initalPR, delta = 0)
      .mapVertices( (id, attr) => (0.0, 0.0) )
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double), (Double, Double)]) = {
      if (edge.srcAttr._2 > tol && edge.dstAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr._1),(edge.srcId, edge.dstAttr._2 * edge.attr._2))
      } else if (edge.srcAttr._2 > tol) { //means dstAttr is not >
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr._1))
      } else if (edge.dstAttr._2 > tol) { //means srcAttr is not >
        Iterator((edge.srcId, edge.dstAttr._2 * edge.attr._2))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage = resetProb / (1.0 - resetProb)

    // Execute a dynamic version of Pregel.
    Pregel(pagerankGraph, initialMessage, maxIter, 
      activeDirection = EdgeDirection.Either)(
      vertexProgram, sendMessage, messageCombiner)
      .mapTriplets(e => e.attr._1) //I don't think it matters which we pick
      .mapVertices((vid, attr) => attr._1)
  } // end of runUntilConvergence

  /**
   * Run an undirected dynamic version of PageRank on a multigraph
   *  returning a graph with vertex attributes containing the list of
   * PageRank and edge attributes containing the list of normalized edge weights.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex a list of pagerank and each edge
   *         containing the list of normalized weights.
   */
  def runCombined[VD: ClassTag, ED: ClassTag](
      graph: Graph[Map[Int,VD], (ED,Int)], numInts: Int, tol: Double, resetProb: Double = 0.15, maxIter: Int = Int.MaxValue): Graph[Map[Int,Double], (Double,Int)] =
  {
    //we need to exchange one message with the info for each edge interval

    def mergedFunc(a:Map[Int,Int], b:Map[Int,Int]): Map[Int,Int] = {
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    //compute degree of each vertex for each interval
    //this should produce a map from interval to degree for each vertex
    //since edge is treated by MultiGraph as undirectional, can use the edge markings
    val degrees: VertexRDD[Map[Int,Int]] = graph.aggregateMessages[Map[Int,Int]](
      ctx => { 
        ctx.sendToSrc(Map(ctx.attr._2 -> 1))
        ctx.sendToDst(Map(ctx.attr._2 -> 1)) 
      },
      mergedFunc,
      TripletFields.All)
  
    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/degree and each vertex with attribute 1.0.
    val pagerankGraph: Graph[Map[Int,(Double,Double)], (Double,Double,Int)] = graph
      // Associate the degree with each vertex for each interval
      .outerJoinVertices(degrees) {
      case (vid, vdata, Some(deg)) => deg
      case (vid, vdata, None) => Map[Int,Int]()
      }
      // Set the weight on the edges based on the degree of that interval
      .mapTriplets( e => (1.0 / e.srcAttr(e.attr._2), 1.0 / e.dstAttr(e.attr._2), e.attr._2) )
      // Set the vertex attributes to (initalPR, delta = 0) for each interval
      .mapVertices( (id, attr) => attr.mapValues { x => (0.0,0.0) } )
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: Map[Int,(Double, Double)], msg: Map[Int,Double]): Map[Int,(Double, Double)] = {
      //need to compute new values for each interval
      //each edge carries a message for one interval,
      //which are combined by the combiner into a hash
      //for each interval in the msg hash, update
      var vals = attr
      msg.foreach { x =>
        val (k,v) = x
        if (vals.contains(k)) {
          val (oldPR, lastDelta) = vals(k)
          val newPR = oldPR + (1.0 - resetProb) * msg(k)
          vals = vals.updated(k,(newPR,newPR-oldPR))
        }
      }
      vals
    }

    def sendMessage(edge: EdgeTriplet[Map[Int,(Double, Double)], (Double, Double, Int)]) = {
      //each edge attribute is supposed to be a triple of (1/degree, 1/degree, year index)
      //each vertex attribute is supposed to be a map of (double,double) for each index
      if (edge.srcAttr(edge.attr._3)._2 > tol && 
        edge.dstAttr(edge.attr._3)._2 > tol) {
        Iterator((edge.dstId, Map((edge.attr._3 -> edge.srcAttr(edge.attr._3)._2 * edge.attr._1))), (edge.srcId, Map((edge.attr._3 -> edge.dstAttr(edge.attr._3)._2 * edge.attr._2))))
      } else if (edge.srcAttr(edge.attr._3)._2 > tol) { 
        Iterator((edge.dstId, Map((edge.attr._3 -> edge.srcAttr(edge.attr._3)._2 * edge.attr._1))))
      } else if (edge.dstAttr(edge.attr._3)._2 > tol) {
        Iterator((edge.srcId, Map((edge.attr._3 -> edge.dstAttr(edge.attr._3)._2 * edge.attr._2))))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Map[Int,Double], b: Map[Int,Double]): Map[Int,Double] = {
     (a.keySet ++ a.keySet).map { i =>
        val count1Val:Double = a.getOrElse(i, 0.0)
        val count2Val:Double = b.getOrElse(i, 0.0)
        i -> (count1Val + count2Val)
      }.toMap
     }

    // The initial message received by all vertices in PageRank
    //has to be a map from every interval index
    var i:Int = 0
    val initialMessage:Map[Int,Double] = (for(i <- 0 to numInts) yield (i -> resetProb / (1.0 - resetProb)))(breakOut)

    // Execute a dynamic version of Pregel.
    Pregel(pagerankGraph, initialMessage, maxIter,
      activeDirection = EdgeDirection.Either)(
      vertexProgram, sendMessage, messageCombiner)
      .mapTriplets(e => (e.attr._1, e.attr._3)) //I don't think it matters which we pick
    //take just the new ranks from vertices, and the indices
      .mapVertices((vid, attr) => attr.mapValues( x => x._1))
  } // end of runUntilConvergence

}
