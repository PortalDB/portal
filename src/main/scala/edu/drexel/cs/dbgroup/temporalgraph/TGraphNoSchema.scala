package edu.drexel.cs.dbgroup.temporalgraph

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

import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps

abstract class TGraphNoSchema[VD: ClassTag, ED: ClassTag](defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraph[VD, ED] {

  val storageLevel = storLevel
  val defaultValue: VD = defValue
  //whether this TGraph is known to be coalesced
  //false means it may or may not be coalesced
  //whereas true means definitely coalesced
  val coalesced: Boolean = coal

  /**
    * An RDD containing the vertices and their associated attributes.
    * @return an RDD containing the vertices in this graph, across all intervals.
    * The vertex attributes are in a Map of Interval->value.
    * The interval is maximal.
    */
  override def verticesAggregated: RDD[(VertexId,Map[Interval, VD])] = {
    vertices.mapValues(y => {var tmp = new Object2ObjectOpenHashMap[Interval,VD](); tmp.put(y._1, y._2); tmp.asInstanceOf[Map[Interval, VD]]})
      .reduceByKey((a: Map[Interval, VD], b: Map[Interval, VD]) => a ++ b)
  }

  /**
    * An RDD containing the edges and their associated attributes.
    * @return an RDD containing the edges in this graph, across all intervals.
    */
  override def edgesAggregated: RDD[((VertexId,VertexId),Map[Interval, ED])] = {
    edges.mapValues(y => {var tmp = new Object2ObjectOpenHashMap[Interval,ED](); tmp.put(y._1, y._2); tmp.asInstanceOf[Map[Interval, ED]]})
      .reduceByKey((a: Map[Interval, ED], b: Map[Interval, ED]) => a ++ b)
  }

  override def aggregate(res: WindowSpecification, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED)(vgroupby: (VertexId, VD) => VertexId = vgb): TGraphNoSchema[VD, ED] = {
    //aggregateByChange requires coalesced tgraph for correctness
    //both produce potentially uncoalesced TGraph
    res match {
      case c : ChangeSpec => coalesce().asInstanceOf[TGraphNoSchema[VD, ED]].aggregateByChange(c, vgroupby, vquant, equant, vAggFunc, eAggFunc)
      case t : TimeSpec => aggregateByTime(t, vgroupby, vquant, equant, vAggFunc, eAggFunc)
      case _ => throw new IllegalArgumentException("unsupported window specification")
    }
  }

  protected def aggregateByChange(c: ChangeSpec, vgroupby: (VertexId, VD) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraphNoSchema[VD, ED]
  protected def aggregateByTime(c: TimeSpec, vgroupby: (VertexId, VD) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraphNoSchema[VD, ED]

  /**
    * Transforms the structural schema of the graph
    * @param emap The mapping function for edges
    * @param vmap The mapping function for vertices
    * @param defaultValue The default value for attribute VD2. Should be something that is not an available value, like Null
    * @return tgraph The transformed graph. The temporal schema is unchanged.
    */
  def map[ED2: ClassTag, VD2: ClassTag](emap: Edge[ED] => ED2, vmap: (VertexId, VD) => VD2, defVal: VD2): TGraphNoSchema[VD2, ED2]

  /**
    * Transforms each vertex attribute in the graph for each time period
    * using the map function. 
    * Special case of general transform, included here for better compatibility with GraphX.
    *
    * @param map the function from a vertex object to a new vertex value
    * @param defaultValue The default value for attribute VD2. Should be something that is not an available value, like Null
    * @tparam VD2 the new vertex data type
    *
    */
  def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2, defVal: VD2)(implicit eq: VD =:= VD2 = null): TGraphNoSchema[VD2, ED]

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
  def mapEdges[ED2: ClassTag](map: (Interval, Edge[ED]) => ED2): TGraphNoSchema[VD, ED2]

  /**
    * Produce a union of two temporal graphs. 
    * @param other The other TGraph
    * @return new TGraph with the union of entities from both graphs within each chronon.
    */
  def union(other: TGraphNoSchema[VD, ED]): TGraphNoSchema[Set[VD], Set[ED]]

  /**
    * Produce the intersection of two temporal graphs.
    * @param other The other TGraph
    * @return new TemporaGraph with the intersection of entities from both graphs within each chronon.
    */
  def intersection(other: TGraphNoSchema[VD, ED]): TGraphNoSchema[Set[VD], Set[ED]]

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

  protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): TGraphNoSchema[V, E]

}

object TGraphNoSchema {
  def computeIntervals[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))]): RDD[Interval] = {
    val dates: RDD[LocalDate] = verts.flatMap{ case (id, (intv, attr)) => List(intv.start, intv.end)}.union(edgs.flatMap { case (ids, (intv, attr)) => List(intv.start, intv.end)}).distinct
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
            if (head._2 == c._2 && head._1.end == c._1.start) (Interval(head._1.start, c._1.end), head._2) :: tail
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
  def constrainEdges[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))]): RDD[((VertexId, VertexId), (Interval, E))] = {
    if (verts.isEmpty) return ProgramContext.sc.emptyRDD[((VertexId, VertexId), (Interval, E))]

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
          ((vid1, vid2), (p, v)) <- iter
          val l1 = m.get(vid1).getOrElse(List[Interval]()).filter(ii => ii.intersects(p))
          val l2 = m.get(vid2).getOrElse(List[Interval]()).filter(ii => ii.intersects(p))
          if l1.size == 1 && l2.size == 1 && l1.head.intersects(l2.head)
        } yield ((vid1, vid2), (Interval(TempGraphOps.maxDate(p.start, m(vid1).find(_.intersects(p)).get.start, m(vid2).find(_.intersects(p)).get.start), TempGraphOps.minDate(p.end, m(vid1).find(_.intersects(p)).get.end, m(vid2).find(_.intersects(p)).get.end)), v))
      }, preservesPartitioning = true)

    } else {
      //coalesce structure first so that edges are not split up
      val coalescV = TGraphNoSchema.coalesceStructure(verts).partitionBy(new HashPartitioner(edgs.getNumPartitions))

      //get edges that are valid for each of their two vertices
      edgs.map(e => (e._1._1, e))
        .join(coalescV) //this creates RDD[(VertexId, (((VertexId, VertexId), (Interval, ED)), Interval))]
        .filter{case (vid, (e, v)) => e._2._1.intersects(v) } //this keeps only matches of vertices and edges where periods overlap
        .map{case (vid, (e, v)) => (e._1._2, (e, v))}       //((e._1, e._2._1), (e._2._2, v))}
        .join(coalescV) //this creates RDD[(VertexId, ((((VertexId, VertexId), (Interval, ED)), Interval), Interval)
        .filter{ case (vid, (e, v)) => e._1._2._1.intersects(v) && e._2.intersects(v)}
        .map{ case (vid, (e, v)) => (e._1._1, (Interval(TempGraphOps.maxDate(v.start, e._1._2._1.start, e._2.start), TempGraphOps.minDate(v.end, e._1._2._1.end, e._2.end)), e._1._2._2))}

    }
  }

}
