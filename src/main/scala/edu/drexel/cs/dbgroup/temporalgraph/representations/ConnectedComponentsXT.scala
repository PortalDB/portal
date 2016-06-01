package edu.drexel.cs.dbgroup.temporalgraph.representations

import java.util.Map

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap

import scala.reflect.ClassTag
import org.apache.spark.graphx._

import scala.collection.breakOut
import scala.collection.immutable.BitSet
import scala.collection.JavaConversions._
import collection.JavaConverters._
//import scala.collection.mutable.HashMap

import edu.drexel.cs.dbgroup.temporalgraph._

//connected components algorithm for temporalgraph
object ConnectedComponentsXT {
  def runHybrid(graph: Graph[BitSet,BitSet], minIndex: Int, maxIndex: Int): Graph[Map[TimeIndex, VertexId], BitSet] = {
    val conGraph: Graph[Map[TimeIndex, VertexId], BitSet] = graph.mapVertices{ case (vid, bset) => mapAsJavaMap(new Int2LongOpenHashMap() ++ bset.map(x => (x,vid))).asInstanceOf[Map[TimeIndex,VertexId]] } //.cache()
   
    def vertexProgram(id: VertexId, attr: Map[TimeIndex, VertexId], msg: Map[TimeIndex, VertexId]): Map[TimeIndex, VertexId] = {
      var vals = new Int2LongOpenHashMap()

      msg.foreach { x =>
        val (k,v) = x
        if (attr.contains(k)) {
          vals.put(k, math.min(v, attr(k)))
        }
      }
      vals.asInstanceOf[Map[TimeIndex, VertexId]]
    }
    
    def sendMessage(edge: EdgeTriplet[Map[TimeIndex, VertexId], BitSet]): Iterator[(VertexId, Map[TimeIndex, VertexId])] = {
      //This is a hack because of a bug in GraphX that
      //does not fetch edge triplet attributes otherwise
      edge.srcAttr
      edge.dstAttr

      edge.attr.flatMap{ k =>
        if (edge.srcAttr.containsKey(k) && edge.dstAttr.containsKey(k) && edge.srcAttr(k) < edge.dstAttr(k))
          Some((edge.dstId, {var tmp = new Int2LongOpenHashMap(); tmp.put(k, edge.srcAttr(k)); tmp.asInstanceOf[Map[TimeIndex, VertexId]]}))
          //Some((edge.dstId, HashMap(k -> edge.srcAttr(k))))
        else if (edge.srcAttr.containsKey(k) && edge.dstAttr.containsKey(k) && edge.srcAttr(k) > edge.dstAttr(k))
          Some((edge.srcId, {var tmp = new Int2LongOpenHashMap(); tmp.put(k, edge.dstAttr(k)); tmp.asInstanceOf[Map[TimeIndex, VertexId]]}))
          //Some((edge.srcId, HashMap(k -> edge.dstAttr(k))))
        else
          None
      }
      .iterator
    }
    
    def messageCombiner(a: Map[TimeIndex, VertexId], b: Map[TimeIndex, VertexId]): Map[TimeIndex, VertexId] = {
      mapAsJavaMap(a ++ b.map { case (index, count) => index -> math.min(count, a.getOrElse(index, Long.MaxValue))})
    }

    val i: Int = 0
    val initialMessage: Map[TimeIndex, VertexId] = {
      var tmpMap = new Int2LongOpenHashMap()
      for(i <- minIndex to maxIndex) {
        tmpMap.put(i, Long.MaxValue)
      }
      tmpMap.asInstanceOf[Map[TimeIndex, VertexId]]
    }

    Pregel(conGraph, initialMessage, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)
  }

}
