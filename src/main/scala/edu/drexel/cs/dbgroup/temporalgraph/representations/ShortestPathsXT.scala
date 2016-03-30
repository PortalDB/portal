package edu.drexel.cs.dbgroup.temporalgraph.representations

import org.apache.spark.graphx._
import scala.reflect.ClassTag
import scala.collection.breakOut

import edu.drexel.cs.dbgroup.temporalgraph._

/**
 * Computes shortest paths to the given set of landmark vertices for temporal graphs
 */
object ShortestPathsXT {
  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type SPMap = Map[VertexId, Int]

  private def makeMap(x: (VertexId, Int)*) = Map(x: _*)

  private def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

  /**
   * Computes shortest paths to the given set of landmark vertices.
   *
   * @tparam ED the edge attribute type (not used in the computation)
   *
   * @param graph the graph for which to compute the shortest paths
   * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
   * landmark.
   *
   * @return a graph where each vertex attribute is a map containing the shortest-path distance to
   * each reachable landmark vertex.
   */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], landmarks: Seq[VertexId]): Graph[SPMap, ED] = {
    val spGraph = graph.mapVertices { (vid, attr) =>
      if (landmarks.contains(vid)) makeMap(vid -> 0) else makeMap()
    }

    val initialMessage = makeMap()

      def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
        addMaps(attr, msg)
      }

      def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
        val newAttr = incrementMap(edge.dstAttr)
        val newAttr2 = incrementMap(edge.srcAttr)

        if (edge.srcAttr != addMaps(newAttr, edge.srcAttr))
          Iterator((edge.srcId, newAttr))
        else if (edge.dstAttr != addMaps(newAttr2, edge.dstAttr))
          Iterator((edge.dstId, newAttr2))
        else
          Iterator.empty
      }

    Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
  }

  /**
   * Computes shortest paths to the given set of landmark vertices
   * for MultiGraph.
   *
   * @tparam ED the edge attribute type (not used in the computation)
   *
   * @param graph the graph for which to compute the shortest paths
   * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
   * landmark.
   *
   * @return a graph where each vertex attribute is a map containing the shortest-path distance to
   * each reachable landmark vertex.
   */
  def runCombined[VD, ED: ClassTag](graph: Graph[Map[TimeIndex, VD], (TimeIndex, ED)], landmarks: Seq[VertexId], numInts: Int): Graph[Map[TimeIndex, SPMap], (TimeIndex, ED)] = {
    val spGraph: Graph[Map[TimeIndex, SPMap], (TimeIndex, ED)] = graph
      // Set the vertex attributes to vertex id for each interval
      .mapVertices { (vid, attr) =>
        attr.mapValues { x =>
          if (landmarks.contains(vid)) makeMap(vid -> 0) else makeMap()
        }
      }

    val initialMessage: Map[TimeIndex, SPMap] = (for (i <- 0 to numInts) yield (i -> makeMap()))(breakOut)

      def addMapsCombined(a: Map[TimeIndex, SPMap], b: Map[TimeIndex, SPMap]): Map[TimeIndex, SPMap] = {
        (a.keySet ++ b.keySet).map { k => 
          k -> addMaps(a.getOrElse(k, makeMap()), b.getOrElse(k, makeMap()))
          }.toMap
      }

      def vertexProgram(id: VertexId, attr: Map[TimeIndex, SPMap], msg: Map[TimeIndex, SPMap]): Map[TimeIndex, SPMap] = {
        //need to compute new shortestPaths to landmark for each interval
        //each edge carries a message for one interval,
        //which are combined by the combiner into a hash
        //for each interval in the msg hash, update
        var vals = attr
        msg.foreach { x =>
          val (k, v) = x
          if (vals.contains(k)) {
            var newMap = addMaps(attr(k), msg(k))
            vals = vals.updated(k, newMap)
          }
        }
        vals
      }

      def sendMessage(edge: EdgeTriplet[Map[TimeIndex, SPMap], (TimeIndex, ED)]): Iterator[(VertexId, Map[TimeIndex, SPMap])] = {
        //each vertex attribute is supposed to be a map of int->spmap for each index
        var yearIndex = edge.attr._1
        var srcSpMap = edge.srcAttr(yearIndex)
        var dstSpMap = edge.dstAttr(yearIndex)

        val newAttr = incrementMap(dstSpMap)
        val newAttr2 = incrementMap(srcSpMap)
        
        if (srcSpMap != addMaps(newAttr, srcSpMap))
          Iterator((edge.srcId, Map(yearIndex -> newAttr)))
        else if (dstSpMap != addMaps(newAttr2, dstSpMap))
          Iterator((edge.dstId, Map(yearIndex -> newAttr2)))
        else
          Iterator.empty
      }

    Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMapsCombined)
  }
}
