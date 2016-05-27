package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.collection.mutable.HashMap
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

  def runHybrid(graph: Graph[BitSet, BitSet], minIndex: Int, maxIndex: Int, tol: Double, resetProb: Double = 0.15, maxIter: Int = Int.MaxValue): Graph[HashMap[TimeIndex,(Double,Double)], HashMap[TimeIndex,(Double,Double)]] =
  {
    def mergeFunc(a:HashMap[TimeIndex,Int], b:HashMap[TimeIndex,Int]): HashMap[TimeIndex,Int] = {
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    def vertexProgram(id: VertexId, attr: HashMap[TimeIndex, (Double,Double)], msg: HashMap[TimeIndex, Double]): HashMap[TimeIndex, (Double,Double)] = {
      val vals = attr.clone
      msg.foreach { x =>
        val (k,v) = x
        if (vals.contains(k)) {
          val (oldPR, lastDelta) = vals(k)
          val newPR = oldPR + (1.0 - resetProb) * msg(k)
          vals.update(k,(newPR,newPR-oldPR))
        }
      }

      vals
    }

    def sendMessage(edge: EdgeTriplet[HashMap[TimeIndex,(Double,Double)], HashMap[TimeIndex, (Double,Double)]]): Iterator[(VertexId, HashMap[TimeIndex,Double])] = {
      //This is a hack because of a bug in GraphX that
      //does not fetch edge triplet attributes otherwise
      edge.srcAttr
      edge.dstAttr

      //need to generate an iterator of messages for each index
      edge.attr.flatMap{ case (k,v) =>
        if (edge.srcAttr(k)._2 > tol &&
          edge.dstAttr(k)._2 > tol) {
          Iterator((edge.dstId, HashMap((k -> edge.srcAttr(k)._2 * v._1))), (edge.srcId, HashMap((k -> edge.dstAttr(k)._2 * v._2))))
        } else if (edge.srcAttr(k)._2 > tol) {
          Some((edge.dstId, HashMap((k -> edge.srcAttr(k)._2 * v._1))))
        } else if (edge.dstAttr(k)._2 > tol) {
          Some((edge.srcId, HashMap((k -> edge.dstAttr(k)._2 * v._2))))
        } else {
          None
        }
      }
        .iterator
    }

    def messageCombiner(a: HashMap[TimeIndex,Double], b: HashMap[TimeIndex,Double]): HashMap[TimeIndex,Double] = {
      a ++ b.map { case (index, count) => index -> (count + a.getOrElse(index,0.0))}
    }
    
    val degs: VertexRDD[HashMap[TimeIndex, Int]] = graph.aggregateMessages[HashMap[TimeIndex, Int]](
      ctx => {
        ctx.sendToSrc(HashMap[TimeIndex,Int]() ++ ctx.attr.seq.map(x => (x,1)))
        ctx.sendToDst(HashMap[TimeIndex,Int]() ++ ctx.attr.seq.map(x => (x,1)))
      },
      mergeFunc, TripletFields.EdgeOnly)
    
    val joined: Graph[HashMap[TimeIndex,Int], BitSet] = graph.outerJoinVertices(degs) {
      case (vid, vdata, Some(deg)) => deg ++ vdata.filter(x => !deg.contains(x)).seq.map(x => (x,0))
      case (vid, vdata, None) => HashMap[TimeIndex,Int]() ++ vdata.seq.map(x => (x,0))
    }

    val withtrips: Graph[HashMap[TimeIndex,Int], HashMap[TimeIndex, (Double,Double)]] = joined.mapTriplets{ e:EdgeTriplet[HashMap[TimeIndex,Int], BitSet] =>
      new HashMap[TimeIndex, (Double, Double)]() ++ e.attr.seq.map(x => (x, (1.0 / e.srcAttr(x), 1.0 / e.dstAttr(x))))}

    val prankGraph:Graph[HashMap[TimeIndex, (Double,Double)], HashMap[TimeIndex, (Double,Double)]] = withtrips.mapVertices( (id,attr) => attr.map{ case (k,x) => (k,(0.0,0.0))}).cache()

    val initialMessage: HashMap[TimeIndex,Double] = (for(i <- minIndex to maxIndex) yield (i -> resetProb / (1.0 - resetProb)))(breakOut)

    Pregel(prankGraph, initialMessage, maxIter, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)

  }
}
