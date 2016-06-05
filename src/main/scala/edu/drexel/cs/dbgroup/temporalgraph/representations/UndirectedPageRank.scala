package edu.drexel.cs.dbgroup.temporalgraph.representations

import java.util.Map

import it.unimi.dsi.fastutil.ints.{Int2DoubleOpenHashMap, Int2IntOpenHashMap, Int2ObjectOpenHashMap}

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.collection.JavaConversions._
import scala.collection.breakOut
import scala.collection.immutable.BitSet

import edu.drexel.cs.dbgroup.temporalgraph._

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

    def sendMessage(edge: EdgeTriplet[(Double, Double), (Double, Double)]): Iterator[(VertexId, Double)] = {
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

  def runHybrid(graph: Graph[BitSet, BitSet], minIndex: Int, maxIndex: Int, tol: Double, resetProb: Double = 0.15, maxIter: Int = Int.MaxValue): Graph[Map[TimeIndex,(Double,Double)], Map[TimeIndex,(Double,Double)]] =
  {
    def mergeFunc(a:Int2IntOpenHashMap, b:Int2IntOpenHashMap): Int2IntOpenHashMap = {
      val itr = a.iterator

      while(itr.hasNext){
        val (index, count) = itr.next()
        b.update(index, (count + b.getOrDefault(index, 0)))
      }
      b
    }

    def vertexProgram(id: VertexId, attr: Int2ObjectOpenHashMap[(Double, Double)], msg: Int2DoubleOpenHashMap): Int2ObjectOpenHashMap[(Double, Double)] = {
      val vals = attr.clone
      msg.foreach { x =>
        val (k,v) = x
        if (attr.contains(k)) {
          val (oldPR, lastDelta) = attr(k)
          val newPR = oldPR + (1.0 - resetProb) * v
          vals.update(k,(newPR,newPR-oldPR))
        }
      }
      vals
    }

    def sendMessage(edge: EdgeTriplet[Int2ObjectOpenHashMap[(Double, Double)], Int2ObjectOpenHashMap[(Double, Double)]]): Iterator[(VertexId, Int2DoubleOpenHashMap)] = {
      //This is a hack because of a bug in GraphX that
      //does not fetch edge triplet attributes otherwise
      edge.srcAttr
      edge.dstAttr

      //need to generate an iterator of messages for each index
      edge.attr.toList.flatMap{ case (k,v) =>
        if (edge.srcAttr(k)._2 > tol && edge.dstAttr(k)._2 > tol) {
          Iterator((edge.dstId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.srcAttr(k)._2 * v._1))),
                (edge.srcId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.dstAttr(k)._2 * v._2))))
        }else if (edge.srcAttr(k)._2 > tol) {
          Some((edge.dstId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.srcAttr(k)._2 * v._1))))
        } else if (edge.dstAttr(k)._2 > tol) {
          Some((edge.srcId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.dstAttr(k)._2 * v._2))))
        } else {
          None
        }
      }
        .iterator
    }

    def messageCombiner(a: Int2DoubleOpenHashMap, b: Int2DoubleOpenHashMap): Int2DoubleOpenHashMap = {
      val itr = a.iterator

      while(itr.hasNext){
        val (index, count) = itr.next()
        b.update(index, (count + b.getOrDefault(index,0.0)))
      }
      b
    }
    
    //TODO: replace these foreach with using the arrays constructor
    val degs: VertexRDD[Int2IntOpenHashMap] = graph.aggregateMessages[Int2IntOpenHashMap](
      ctx => {
        ctx.sendToSrc{val tmp = new Int2IntOpenHashMap(); ctx.attr.seq.foreach(x => tmp.put(x,1)); tmp}
        ctx.sendToDst{val tmp = new Int2IntOpenHashMap(); ctx.attr.seq.foreach(x => tmp.put(x,1)); tmp}
      },
      mergeFunc, TripletFields.EdgeOnly)
    
    val joined: Graph[Int2IntOpenHashMap, BitSet] = graph.outerJoinVertices(degs) {
      case (vid, vdata, Some(deg)) => vdata.filter(x => !deg.contains(x)).seq.foreach(x => deg.put(x,0)); deg
      case (vid, vdata, None) => val tmp = new Int2IntOpenHashMap(); vdata.seq.foreach(x => tmp.put(x,0)); tmp
    }

    val withtrips: Graph[Int2IntOpenHashMap, Int2ObjectOpenHashMap[(Double, Double)]] = joined.mapTriplets{ e:EdgeTriplet[Int2IntOpenHashMap, BitSet] =>
      val tmp = new Int2ObjectOpenHashMap[(Double, Double)](); e.attr.seq.foreach(x => tmp.put(x, (1.0 / e.srcAttr(x), 1.0 / e.dstAttr(x)))); tmp}

    val prankGraph:Graph[Int2ObjectOpenHashMap[(Double, Double)], Int2ObjectOpenHashMap[(Double, Double)]]
    = withtrips.mapVertices( (id,attr) =>
      {val tmp = new Int2ObjectOpenHashMap[(Double, Double)](); attr.foreach(x => tmp.put(x._1, (0.0,0.0))); tmp}).cache()

    val initialMessage: Int2DoubleOpenHashMap = {
      val tmpMap = new Int2DoubleOpenHashMap()

      for(i <- minIndex to maxIndex) {
        tmpMap.put(i, resetProb / (1.0 - resetProb))
      }
      tmpMap
    }

    Pregel(prankGraph, initialMessage, maxIter, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)
    .asInstanceOf[Graph[Map[TimeIndex,(Double,Double)], Map[TimeIndex,(Double,Double)]]]
  }
}
