package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.collection.immutable.Map
import scala.collection.breakOut

import edu.drexel.cs.dbgroup.temporalgraph._

//connected components algorithm for temporalgraph
object ConnectedComponentsXT {
  /**
   * Run connected components algorithm on a multigraph
   * return a graph with the vertex value containing the lowest vertex
   * id in the connected component containing that vertex.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute ConnectedComponents
   * @param numInts the number of intervals in the Multigraph
   *
   * @return the graph where each vertex attribute is a list of
   * the smallest vertex in each connected component for Intervals
   * in which the vertex appears
   */
  def runCombined[VD, ED: ClassTag](graph: Graph[Map[TimeIndex, VD], (TimeIndex, ED)], numInts: Int): Graph[Map[TimeIndex, VertexId], (TimeIndex, ED)] =
    {
      // Initialize the pagerankGraph with each edge attribute
      // having weight 1/degree and each vertex with attribute 1.0.
      val ccGraph: Graph[Map[TimeIndex, VertexId], (TimeIndex, ED)] = graph
        // Set the vertex attributes to vertex id for each interval
        .mapVertices((id, attr) => attr.mapValues { x => id })
        .cache();

        // Define the three functions needed to implement ConnectedComponents in the GraphX version of Pregel
        def vertexProgram(id: VertexId, attr: Map[TimeIndex, VertexId], msg: Map[TimeIndex, VertexId]): Map[TimeIndex, VertexId] = {
          //need to compute new values for each interval
          //each edge carries a message for one interval,
          //which are combined by the combiner into a hash
          //for each interval in the msg hash, update
          var vals = attr
          msg.foreach { x =>
            val (k, v) = x
            if (vals.contains(k)) {
              val cc = math.min(attr(k), msg(k))
              vals = vals.updated(k, cc)
            }
          }
          vals
        }

        def sendMessage(edge: EdgeTriplet[Map[TimeIndex, VertexId], (TimeIndex, ED)]): Iterator[(VertexId, Map[TimeIndex, VertexId])] = {
          //each vertex attribute is supposed to be a map of int->int for each index
          var yearIndex = edge.attr._1

          if (edge.srcAttr(yearIndex) < edge.dstAttr(yearIndex)) {
            Iterator((edge.dstId, Map(yearIndex -> edge.srcAttr(yearIndex))))
          } else if (edge.srcAttr(yearIndex) > edge.dstAttr(yearIndex)) {
            Iterator((edge.srcId, Map(yearIndex -> edge.dstAttr(yearIndex))))
          } else {
            Iterator.empty
          }
        }

        def messageCombiner(a: Map[TimeIndex, VertexId], b: Map[TimeIndex, VertexId]): Map[TimeIndex, VertexId] = {
          (a.keySet ++ a.keySet).map { i =>
            i -> math.min(a.getOrElse(i, Long.MaxValue), b.getOrElse(i, Long.MaxValue))
          }.toMap
        }

      // The initial message received by all vertices in ConnectedComponents
      val initialMessage: Map[TimeIndex, VertexId] = (for (i <- 0 to numInts) yield (i -> Long.MaxValue))(breakOut)
      
      // Execute a dynamic version of Pregel.
      Pregel(ccGraph, initialMessage,
        activeDirection = EdgeDirection.Either)(
          vertexProgram, sendMessage, messageCombiner)
    } // end of runUntilConvergence

}
