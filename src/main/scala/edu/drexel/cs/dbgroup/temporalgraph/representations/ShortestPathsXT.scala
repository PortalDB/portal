package edu.drexel.cs.dbgroup.temporalgraph.representations

import java.util.Map

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.breakOut
import org.apache.spark.graphx._

import scala.reflect.ClassTag
import edu.drexel.cs.dbgroup.temporalgraph._
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap

/**
  * Computes shortest paths to the given set of landmark vertices for temporal graphs
  * treating the graph as undirected
 */
object ShortestPathsXT extends Serializable {
  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type SPMap = Map[VertexId, Int]

  private def makeMap(x: (VertexId, Int)*) = {
    val itr = x.iterator;
    var tmpMap = new Long2IntOpenHashMap()

    while (itr.hasNext) {
      val k = itr.next()
      tmpMap.put(k._1, k._2)
    }
    tmpMap.asInstanceOf[Map[VertexId, Int]]
  }

  private def incrementMap(spmap: SPMap): SPMap = {
    val itr = spmap.entrySet().iterator
    var tmpMap = new Long2IntOpenHashMap()

    while (itr.hasNext) {
      val entry = itr.next()
      val k = entry.getKey()
      val v = entry.getValue()

      tmpMap.put(k, v+1)
    }
    tmpMap.asInstanceOf[Map[VertexId, Int]]
  }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrDefault(k, Int.MaxValue), spmap2.getOrDefault(k, Int.MaxValue))
    }.toMap[VertexId, Int]

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

}
