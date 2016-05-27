package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.collection.breakOut
import scala.collection.immutable.BitSet
import scala.collection.mutable.HashMap

import edu.drexel.cs.dbgroup.temporalgraph._

//connected components algorithm for temporalgraph
object ConnectedComponentsXT {
  def runHybrid(graph: Graph[BitSet,BitSet], minIndex: Int, maxIndex: Int): Graph[HashMap[TimeIndex, VertexId], BitSet] = {
    val conGraph: Graph[HashMap[TimeIndex, VertexId], BitSet] = graph.mapVertices{ case (vid, bset) => HashMap[TimeIndex,VertexId]() ++ bset.map(x => (x,vid))} //.cache()
   
    def vertexProgram(id: VertexId, attr: HashMap[TimeIndex, VertexId], msg: HashMap[TimeIndex, VertexId]): HashMap[TimeIndex, VertexId] = {
      val vals = attr.clone
      msg.foreach { x =>
        val (k,v) = x
        if (vals.contains(k)) {
          vals.update(k, math.min(v, vals(k)))
        }
      }
      vals
    }
    
    def sendMessage(edge: EdgeTriplet[HashMap[TimeIndex, VertexId], BitSet]): Iterator[(VertexId, HashMap[TimeIndex, VertexId])] = {
      //This is a hack because of a bug in GraphX that
      //does not fetch edge triplet attributes otherwise
      edge.srcAttr
      edge.dstAttr

      edge.attr.flatMap{ k =>
        if (edge.srcAttr(k) < edge.dstAttr(k))
          Some((edge.dstId, HashMap(k -> edge.srcAttr(k))))
        else if (edge.srcAttr(k) > edge.dstAttr(k))
          Some((edge.srcId, HashMap(k -> edge.dstAttr(k))))
        else
          None
      }
      .iterator
    }
    
    def messageCombiner(a: HashMap[TimeIndex, VertexId], b: HashMap[TimeIndex, VertexId]): HashMap[TimeIndex, VertexId] = {
      a ++ b.map { case (index, count) => index -> math.min(count, a.getOrElse(index, Long.MaxValue))}
    }

    val i: Int = 0
    val initialMessage: HashMap[TimeIndex, VertexId] = (for(i <- minIndex to maxIndex) yield (i -> Long.MaxValue))(breakOut)

    Pregel(conGraph, initialMessage, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)
  }

}
