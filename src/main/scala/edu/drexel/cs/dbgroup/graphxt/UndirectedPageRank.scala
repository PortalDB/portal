package edu.drexel.cs.dbgroup.graphxt

import scala.reflect.ClassTag
import org.apache.spark.graphx._

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
  def runUntilConvergence[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): Graph[Double, Double] =
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
    Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Either)(
      vertexProgram, sendMessage, messageCombiner)
      .mapTriplets(e => e.attr._1) //I don't think it matters which we pick
      .mapVertices((vid, attr) => attr._1)
  } // end of runUntilConvergence

}
