package edu.drexel.cs.dbgroup.temporalgraph.representations

import java.util.Map

import it.unimi.dsi.fastutil.ints.{Int2DoubleOpenHashMap, Int2IntOpenHashMap, Int2ObjectOpenHashMap}

import scala.reflect.ClassTag
import org.apache.spark.graphx._
//import scala.collection.mutable.HashMap
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
    def mergeFunc(a:Map[TimeIndex,Int], b:Map[TimeIndex,Int]): Map[TimeIndex,Int] = {
      val tmp = b.map{ case (index,count) => index -> (count + a.getOrDefault(index, 0)) }
      mapAsJavaMap(a ++ b)
        //a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    def vertexProgram(id: VertexId, attr: Map[TimeIndex, (Double,Double)], msg: Map[TimeIndex, Double]): Map[TimeIndex, (Double,Double)] = {
      val vals = attr.clone.asInstanceOf[Map[TimeIndex, (Double,Double)]]
      msg.foreach { x =>
        val (k,v) = x
        if (vals.containsKey(k)) {
          val (oldPR, lastDelta) = vals(k)
          val newPR = oldPR + (1.0 - resetProb) * msg(k)
          vals.update(k,(newPR,newPR-oldPR))
        }
      }

      vals
    }

    def sendMessage(edge: EdgeTriplet[Map[TimeIndex,(Double,Double)], Map[TimeIndex, (Double,Double)]]): Iterator[(VertexId, Map[TimeIndex,Double])] = {
      //This is a hack because of a bug in GraphX that
      //does not fetch edge triplet attributes otherwise
      edge.srcAttr
      edge.dstAttr

      //need to generate an iterator of messages for each index
      edge.attr.flatMap{ case (k,v) =>
        if (edge.srcAttr(k)._2 > tol && edge.dstAttr(k)._2 > tol) {
          Iterator((edge.dstId, {var tmp = new Int2DoubleOpenHashMap(); tmp.put(k, edge.srcAttr(k)._2 * v._1); tmp.asInstanceOf[Map[TimeIndex,Double]] } ),
                (edge.srcId, {var tmp = new Int2DoubleOpenHashMap(); tmp.put(k, edge.dstAttr(k)._2 * v._2); tmp.asInstanceOf[Map[TimeIndex,Double]] }))
          //Iterator((edge.dstId, HashMap((k -> edge.srcAttr(k)._2 * v._1))), (edge.srcId, HashMap((k -> edge.dstAttr(k)._2 * v._2))))
        }else if (edge.srcAttr(k)._2 > tol) {
          Some((edge.dstId, {var tmp = new Int2DoubleOpenHashMap(); tmp.put(k, edge.srcAttr(k)._2 * v._1); tmp.asInstanceOf[Map[TimeIndex,Double]] }))
          //Some((edge.dstId, HashMap((k -> edge.srcAttr(k)._2 * v._1))))
        } else if (edge.dstAttr(k)._2 > tol) {
          Some((edge.srcId, {var tmp = new Int2DoubleOpenHashMap(); tmp.put(k, edge.dstAttr(k)._2 * v._2); tmp.asInstanceOf[Map[TimeIndex,Double]] }))
          //Some((edge.srcId, HashMap((k -> edge.dstAttr(k)._2 * v._2))))
        } else {
          None
        }
      }
        .iterator
    }

    def messageCombiner(a: Map[TimeIndex,Double], b: Map[TimeIndex,Double]): Map[TimeIndex,Double] = {
      val tmp = b.map{ case (index, count) => index -> (count + a.getOrDefault(index,0.0))}
      mapAsJavaMap(a ++ b)
    }
    
    val degs: VertexRDD[Map[TimeIndex, Int]] = graph.aggregateMessages[Map[TimeIndex, Int]](
      ctx => {
        ctx.sendToSrc{var tmp = new Int2IntOpenHashMap(); (tmp ++ ctx.attr.seq.map(x => (x,1))).asInstanceOf[Map[TimeIndex, Int]]}
        ctx.sendToDst{var tmp = new Int2IntOpenHashMap(); (tmp ++ ctx.attr.seq.map(x => (x,1))).asInstanceOf[Map[TimeIndex, Int]]}
        //ctx.sendToSrc(HashMap[TimeIndex,Int]() ++ ctx.attr.seq.map(x => (x,1)))
        //ctx.sendToDst(HashMap[TimeIndex,Int]() ++ ctx.attr.seq.map(x => (x,1)))
      },
      mergeFunc, TripletFields.EdgeOnly)
    
    val joined: Graph[Map[TimeIndex,Int], BitSet] = graph.outerJoinVertices(degs) {
      case (vid, vdata, Some(deg)) => deg ++ vdata.filter(x => !deg.contains(x)).seq.map(x => (x,0))
      case (vid, vdata, None) => {var tmp = new Int2IntOpenHashMap(); (tmp ++ vdata.seq.map(x => (x,0))).asInstanceOf[Map[TimeIndex, Int]]}
    }

    val withtrips: Graph[Map[TimeIndex,Int], Map[TimeIndex, (Double,Double)]] = joined.mapTriplets{ e:EdgeTriplet[Map[TimeIndex,Int], BitSet] =>
      (new Int2ObjectOpenHashMap[(Double, Double)]() ++ e.attr.seq.map(x => (x, (1.0 / e.srcAttr(x), 1.0 / e.dstAttr(x))))).asInstanceOf[Map[TimeIndex, (Double, Double)]]}
      //new HashMap[TimeIndex, (Double, Double)]() ++ e.attr.seq.map(x => (x, (1.0 / e.srcAttr(x), 1.0 / e.dstAttr(x))))}

    val prankGraph:Graph[Map[TimeIndex, (Double,Double)], Map[TimeIndex, (Double,Double)]] = withtrips.mapVertices( (id,attr) =>
      attr.map{ case (k,x) => k -> (0.0,0.0) }.asInstanceOf[Map[TimeIndex, (Double, Double)]]).cache()

    val initialMessage: Map[TimeIndex,Double] = {
      var tmpMap = new Int2DoubleOpenHashMap()

      for(i <- minIndex to maxIndex) {
        tmpMap.put(i, resetProb / (1.0 - resetProb))
      }
      tmpMap.asInstanceOf[Map[TimeIndex, Double]]
    }

    Pregel(prankGraph, initialMessage, maxIter, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)

  }
}
