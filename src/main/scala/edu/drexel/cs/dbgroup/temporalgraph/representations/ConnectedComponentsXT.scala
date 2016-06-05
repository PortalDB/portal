package edu.drexel.cs.dbgroup.temporalgraph.representations

import java.util.{HashSet, Map}

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap

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
    val conGraph: Graph[Int2LongOpenHashMap, BitSet]
    = graph.mapVertices{ case (vid, bset) => val tmp = new Int2LongOpenHashMap(); bset.foreach(x => tmp.put(x,vid)); tmp} //.cache()

    def vertexProgram(id: VertexId, attr: Int2LongOpenHashMap, msg: Int2LongOpenHashMap): Int2LongOpenHashMap = {
      var vals = attr.clone()

      msg.foreach { x =>
        val (k,v) = x
        if (attr.contains(k)) {
          vals.update(k, math.min(v, attr(k)))
        }
      }
      vals
    }
    
    def sendMessage(edge: EdgeTriplet[Int2LongOpenHashMap, BitSet]): Iterator[(VertexId, Int2LongOpenHashMap)] = {
      //This is a hack because of a bug in GraphX that
      //does not fetch edge triplet attributes otherwise
      edge.srcAttr
      edge.dstAttr

      edge.attr.flatMap{ k =>
        if (edge.srcAttr(k) < edge.dstAttr(k))
          Some((edge.dstId, {var tmp = new Int2LongOpenHashMap(); tmp.put(k, edge.srcAttr.get(k)); tmp}))
          //Some((edge.dstId, HashMap(k -> edge.srcAttr(k))))
        else if (edge.srcAttr(k) > edge.dstAttr(k))
          Some((edge.srcId, {var tmp = new Int2LongOpenHashMap(); tmp.put(k, edge.dstAttr.get(k)); tmp}))
          //Some((edge.srcId, HashMap(k -> edge.dstAttr(k))))
        else
          None
      }
      .iterator
    }
    
    def messageCombiner(a: Int2LongOpenHashMap, b: Int2LongOpenHashMap): Int2LongOpenHashMap = {
      val itr = a.iterator

      while(itr.hasNext){
        val (k,v) = itr.next()
        b.update(k, math.min(v, b.getOrDefault(k, Long.MaxValue)))
      }
      b
    }

    val i: Int = 0
    val initialMessage: Int2LongOpenHashMap = {
      var tmpMap = new Int2LongOpenHashMap()
      for(i <- minIndex to maxIndex) {
        tmpMap.put(i, Long.MaxValue)
      }
      tmpMap
    }

    Pregel(conGraph, initialMessage, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner).asInstanceOf[Graph[Map[TimeIndex, VertexId], BitSet]]

  }

}
