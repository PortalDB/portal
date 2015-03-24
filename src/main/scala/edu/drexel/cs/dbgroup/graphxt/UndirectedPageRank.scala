package edu.drexel.cs.dbgroup.graphxt

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.collection.immutable.HashMap
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
      graph: Graph[(VD, Seq[Int]), (ED,Int)], numInts: Int, tol: Double, resetProb: Double = 0.15): Graph[(Seq[Double], Seq[Int]), (Double,Int)] =
  {
    //we need to exchange one message with the info for each edge interval

    //compute degree of each vertex for each interval
    //this should produce a map from interval to degree for each vertex
    //since edge is treated by MultiGraph as undirectional, can use the edge markings
    val degrees: VertexRDD[HashMap[Int,Int]] = graph.aggregateMessages[HashMap[Int,Int]](
      ctx => { 
        ctx.sendToSrc(HashMap(ctx.attr._2 -> 1))
        ctx.sendToDst(HashMap(ctx.attr._2 -> 1)) 
      },
      (a,b) => a.merged(b)({ case ((k,v1),(_,v2)) => (k,v1+v2) }), 
      TripletFields.None)
  
    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/degree and each vertex with attribute 1.0.
    val pagerankGraph: Graph[(Seq[(Double,Double)], Seq[Int]), (Double,Double,Int)] = graph
      // Associate the degree with each vertex for each interval
      .outerJoinVertices(degrees) {
      case (vid, vdata, Some(deg)) => (vdata._2, deg)
      case (vid, vdata, None) => (vdata._2,Map[Int,Int]())
      }
      // Set the weight on the edges based on the degree of that interval
      .mapTriplets( e => (1.0 / e.srcAttr._2(e.attr._2), 1.0 / e.dstAttr._2(e.attr._2), e.attr._2) )
      // Set the vertex attributes to (initalPR, delta = 0) for each interval
      .mapVertices( (id, attr) => ( Seq.fill(attr._1.size)((0.0,0.0)), attr._1) )
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Seq[(Double, Double)], Seq[Int]), msg: Map[Int,Double]): (Seq[(Double, Double)],Seq[Int]) = {
      //need to compute new values for each interval
      //each edge carries a message for one interval,
      //which are combined by the combiner into a hash
      var (vals, intvs) = attr
      //for each interval in the msg hash, update
      msg.foreach { x =>
        val (k,v) = x
        if (vals.contains(k)) {
          val (oldPR, lastDelta) = vals(k)
          val newPR = oldPR + (1.0 - resetProb) * msg(k)
          vals = vals.patch(k,Seq((newPR, newPR - oldPR)), 1)
        }
      }
      (vals,intvs)
    }

    def sendMessage(edge: EdgeTriplet[(Seq[(Double, Double)], Seq[Int]), (Double, Double, Int)]) = {
      if (edge.srcAttr._1(edge.attr._3)._2 > tol && edge.dstAttr._1(edge.attr._3)._2 > tol) {
        Iterator((edge.dstId, Map((edge.attr._3 -> edge.srcAttr._1(edge.attr._3)._2 * edge.attr._1))), (edge.srcId, Map((edge.attr._3 -> edge.dstAttr._1(edge.attr._3)._2 * edge.attr._2))))
      } else if (edge.srcAttr._1(edge.attr._3)._2 > tol) { 
        Iterator((edge.dstId, Map((edge.attr._3 -> edge.srcAttr._1(edge.attr._3)._2 * edge.attr._1))))
      } else if (edge.dstAttr._1(edge.attr._3)._2 > tol) {
        Iterator((edge.srcId, Map((edge.attr._3 -> edge.dstAttr._1(edge.attr._3)._2 * edge.attr._2))))
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
    Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Either)(
      vertexProgram, sendMessage, messageCombiner)
      .mapTriplets(e => (e.attr._1, e.attr._3)) //I don't think it matters which we pick
    //take just the new ranks from vertices, and the indices
      .mapVertices((vid, attr) => (attr._1.map(x => x._1) , attr._2))
  } // end of runUntilConvergence

}
