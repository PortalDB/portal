package edu.drexel.cs.dbgroup.portal

import java.util.Map
import java.time.LocalDate

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.{HashPartitioner,RangePartitioner}

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap

import edu.drexel.cs.dbgroup.portal.util.TempGraphOps

abstract class TGraphNoSchema[VD: ClassTag, ED: ClassTag](defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraph[VD, ED] {
  val storageLevel = storLevel
  val defaultValue: VD = defValue
  //whether this TGraph is known to be coalesced
  //false means it may or may not be coalesced
  //whereas true means definitely coalesced
  val coalesced: Boolean = coal

  override def createTemporalNodes(res: WindowSpecification, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraphNoSchema[VD, ED]={
    res match {
      case c : ChangeSpec => coalesce().asInstanceOf[TGraphNoSchema[VD,ED]].createTemporalByChange(c, vquant, equant, vAggFunc, eAggFunc)
      case t : TimeSpec => coalesce().asInstanceOf[TGraphNoSchema[VD,ED]].createTemporalByTime(t, vquant, equant, vAggFunc, eAggFunc)
      case _ => throw new IllegalArgumentException("unsupported window specification")
    }
  }

  protected def createTemporalByChange(c: ChangeSpec, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraphNoSchema[VD, ED]
  protected def createTemporalByTime(c: TimeSpec, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraphNoSchema[VD, ED]

  /**
    * Transforms each vertex attribute in the graph for each time period
    * using the map function. 
    * Special case of general transform, included here for better compatibility with GraphX.
    *
    * @param map the function from a vertex object to a new vertex value
    * @tparam VD2 the new vertex data type
    *
    */
  def vmap[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2, defVal: VD2)(implicit eq: VD =:= VD2 = null): TGraphNoSchema[VD2, ED]

  /**
   * Transforms each edge attribute in the graph using the map function.  The map function is not
   * passed the vertex value for the vertices adjacent to the edge.  If vertex values are desired,
   * use `mapTriplets`.
   * Special case of general transform, included here for better compatibility with GraphX.
   *
   * @param map the function from an edge object with a time index to a new edge value.
   *
   * @tparam ED2 the new edge data type
   *
   */
  def emap[ED2: ClassTag](map: TEdge[ED] => ED2): TGraphNoSchema[VD, ED2]

  /**
    * Produce a union of two temporal graphs. 
    * @param other The other TGraph
    * @param vFunc The aggregate function on vertices
    * @param eFunc The aggregate function on edges
    * @return new TGraph with the union of entities from both graphs within each chronon.
    */
  def union(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD , eFunc: (ED, ED) => ED): TGraphNoSchema[VD, ED]


  /**
    * Produce the difference of two temporal graphs.
    * @param other The other TGraph
    * @return new TemporaGraph with the diffrence of entities from both graphs within each chronon.
    */
  def difference(other: TGraphNoSchema[VD, ED]): TGraphNoSchema[VD, ED]


  /**
    * Produce the intersection of two temporal graphs.
    * @param other The other TGraph
    * @param vFunc The aggregate function on vertices
    * @param eFunc The aggregate function on edges
    * @return new TemporaGraph with the intersection of entities from both graphs within each chronon.
    */
  def intersection(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TGraphNoSchema[VD, ED]

  /**
    * The analytics methods
    */

  /**
    * Run pagerank on all intervals. It is up to the implementation to run sequantially,
    * in parallel, incrementally, etc. The number of iterations will depend on both
    * the numIter argument and the rate of convergence, whichever occurs first.
    * @param uni Treat the graph as undirected or directed. true = directed
    * @param tol epsilon, measure of convergence
    * @param resetProb probability of reset/jump
    * @param numIter number of iterations of the algorithm to run. If omitted, will run
    * until convergence of the tol argument.
    * @return New graph with pageranks for each interval (coalesced)
    */
  def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): TGraphNoSchema[(VD,Double), ED]

  /**
   * Run connected components algorithm on a temporal graph
   * return a graph with the vertex value containing the lowest vertex
   * id in the connected component containing that vertex.
   *
   * @return New graph with vertex attribute the id of 
   * the smallest vertex in each connected component 
   * for Intervals in which the vertex appears
   */
  def connectedComponents(): TGraphNoSchema[(VD,VertexId), ED]
  
  /**
   * Computes shortest paths to the given set of landmark vertices.
   * @param landmarks the list of landmark vertex ids to which shortest paths will be computed
    * @param uni Treat the graph as undirected or directed. true = directed
    * @return Graph with vertices where each vertex attribute
   * is the shortest-path distance to each reachable landmark vertex.
   */
  def shortestPaths(uni: Boolean, landmarks: Seq[VertexId]): TGraphNoSchema[(VD,Map[VertexId, Int]), ED]

  /**
    * Compute the number of triangles passing through each vertex.
    * 
    * @return Graph with vertices where each vertex attribute has
    * added a number of triangles.
    */
  def triangleCount(): TGraphNoSchema[(VD,Int), ED]

  /**
    * Compute the clustering coefficient of each vertex,
    * which is equal to the number of triangles that pass through it
    * divided by k*(k-1), where k is the vertex degree
    * 
    * @return Graph with vertices where each vertex attribute has
    * added a clustering coefficient between 0.0 and 1.0.
    */
  def clusteringCoefficient(): TGraphNoSchema[(VD,Double), ED]

  /**
   * Aggregates values from the neighboring edges and vertices of each vertex, for each representative graph. 
   * Unlike in GraphX, this returns a new graph, not an RDD. The user-supplied
   * `sendMsg` function is invoked on each edge of the graph, generating 0 or more messages to be
   * sent to either vertex in the edge. The `mergeMsg` function is then used to combine all messages
   * destined to the same vertex.
   *
   * @tparam A the type of message to be sent to each vertex
   *
   * @param sendMsg runs on each edge, sending messages to neighboring vertices using the
   *   [[EdgeContext]].
   * @param mergeMsg used to combine messages from `sendMsg` destined to the same vertex. This
   *   combiner should be commutative and associative.
   * @param tripletFields which fields should be included in the [[EdgeContext]] passed to the
   *   `sendMsg` function. If not all fields are needed, specifying this can improve performance.
   *
   */
//TODO: can we have a simpler version where there's a predicate on the vertex and a predicate on the edge and the edge direction and the message and the aggregation function
  def aggregateMessages[A: ClassTag](sendMsg: TEdgeTriplet[VD,ED] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A, defVal: A, tripletFields: TripletFields = TripletFields.All): TGraphNoSchema[(VD, A), ED]

  protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): TGraphNoSchema[V, E]
  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[TEdge[E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): TGraphNoSchema[V,E]
}

object TGraphNoSchema {
  def computeIntervals[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[TEdge[E]]): RDD[Interval] = {
    val dates: RDD[LocalDate] = verts.flatMap{ case (id, (intv, attr)) => List(intv.start, intv.end)}.union(edgs.flatMap { case e => List(e.interval.start, e.interval.end)}).distinct
    implicit val ord = TempGraphOps.dateOrdering
    dates.sortBy(c => c, true, 1).sliding(2).map(lst => Interval(lst(0), lst(1)))
  }

  /*
   * given an RDD where for a key there is a value over time period
   * coalesce consecutive periods with the same value
   * i.e. (1, (1-3, "blah")) and (1, (3-4, "blah")) become
   * single tuple (1, (1-4, "blah"))
   * Warning: This is a very expensive operation, use sparingly
   */
  //TODO: this method is not specific to nonschema relations, move to util class
  def coalesce[K: Ordering : ClassTag, V: ClassTag](rdd: RDD[(K, (Interval, V))]): RDD[(K, (Interval, V))] = {
    implicit val ord = TempGraphOps.dateOrdering
    //it's faster if we partition first
    //TODO: in some cases RangePartition provides better performance, but in others it makes it too slow
    //rdd.partitionBy(new org.apache.spark.RangePartitioner(rdd.getNumPartitions, rdd))
    rdd.partitionBy(new HashPartitioner(rdd.getNumPartitions))
      .groupByKey.mapValues{ seq =>  //groupbykey produces RDD[(K, Seq[(p, V)])]
      seq.toSeq.sortBy(x => x._1.start)
        .foldLeft(List[(Interval, V)]()){ (r,c) => r match {
          case head :: tail =>
            if (head._2 == c._2 && (head._1.end == c._1.start || head._1.intersects(c._1))) (Interval(head._1.start, c._1.end), head._2) :: tail
            else c :: r
          case Nil => List(c)
        }
      }}.flatMap{ case (k,v) => v.map(x => (k, x))}
  }

  /*
   * given an RDD where for a key there is a value over time period
   * coalesce consecutive periods for the same keys regardless of values
   * i.e. (1, (1-3, "blah")) and (1, (3-4, "bob")) become
   * single tuple (1, 1-4)
   * Warning: This is a very expensive operation, use sparingly
   */
  def coalesceStructure[K: ClassTag, V: ClassTag](rdd: RDD[(K, (Interval, V))]): RDD[(K, Interval)] = {
    implicit val ord = TempGraphOps.dateOrdering
    rdd.mapValues(_._1)
      .partitionBy(new HashPartitioner(rdd.getNumPartitions))
      .groupByKey.mapValues{ seq =>
      seq.toSeq.sortBy(x => x.start)
        .foldLeft(List[Interval]()){ (r,c) => r match {
          case head :: tail =>
            if (head.end == c.start) Interval(head.start, c.end) :: tail
            else c :: r
          case Nil => List(c)
        }
    }}.flatMap{ case (k,v) => v.map(x => (k, x))}
  }

  /*
   * Given an RDD of vertices and an RDD of edges,
   * remove the edges for which there is no src or dst vertex
   * at the indicated time period,
   * shorten the periods as needed to meet the integrity constraint.
   * Warning: This is a very expensive operation, use only when needed.
   */
  def constrainEdges[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[TEdge[E]]): RDD[TEdge[E]] = {
    //if we don't have many vertices, we can do a better job with broadcasts
    //FIXME: what should this number be? it should depend on memory size
    //TODO: pull this logic into query optimization or use DataFrames so that sql can do it automatically
    if (verts.countApprox(2).getFinalValue.high < 10000000) {
      //coalesce before collect
      //TODO: use future so that this broadcast only happens if we need edges
      val bverts = ProgramContext.sc.broadcast(verts.aggregateByKey(List[Interval]())(seqOp = (u,v) => v._1 :: u, combOp = (u1,u2) => u1 ++ u2).mapValues(_.sorted.foldLeft(List[Interval]()){ (r,c) => r match {
        case head :: tail =>
          if (head.end == c.start) Interval(head.start, c.end) :: tail
          else c :: r
        case Nil => List(c)
      }}).collectAsMap())

      edgs.mapPartitions({ iter =>
        val m: scala.collection.Map[VertexId, List[Interval]] = bverts.value
        for {
          e <- iter
          l1 = m.get(e.srcId).getOrElse(List[Interval]()).filter(ii => ii.intersects(e.interval))
          l2 = m.get(e.dstId).getOrElse(List[Interval]()).filter(ii => ii.intersects(e.interval))
          if l1.size == 1 && l2.size == 1 && l1.head.intersects(l2.head)
        } yield TEdge.apply((e.eId,e.srcId, e.dstId), (Interval(TempGraphOps.maxDate(e.interval.start, m(e.srcId).find(_.intersects(e.interval)).get.start, m(e.dstId).find(_.intersects(e.interval)).get.start), TempGraphOps.minDate(e.interval.end, m(e.srcId).find(_.intersects(e.interval)).get.end, m(e.dstId).find(_.intersects(e.interval)).get.end)), e.attr))
      }, preservesPartitioning = true)

    } else {
      //coalesce structure first so that edges are not split up
      val coalescV = TGraphNoSchema.coalesceStructure(verts).partitionBy(new HashPartitioner(edgs.getNumPartitions))

      //get edges that are valid for each of their two vertices
      edgs.map(e => (e.srcId, e))
        .join(coalescV) //this creates RDD[(VertexId, (TEdge[E], Interval))]
        .filter{case (vid, (e, v)) => e.interval.intersects(v) } //this keeps only matches of vertices and edges where periods overlap
        .map{case (vid, (e, v)) => (e.dstId, (e, v))}       //((e._1, e._2._1), (e._2._2, v))}
        .join(coalescV) //this creates RDD[(VertexId, ((TEdge[E], Interval), Interval)
        .filter{ case (vid, (e, v)) => e._1.interval.intersects(v) && e._2.intersects(v)}
        .map{ case (vid, (e, v)) =>
          TEdge.apply((e._1.eId, e._1.srcId, e._1.dstId),
            ((Interval(TempGraphOps.maxDate(v.start, e._1.interval.start, e._2.start), TempGraphOps.minDate(v.end, e._1.interval.end, e._2.end)), e._1.attr)))}

    }
  }

}
