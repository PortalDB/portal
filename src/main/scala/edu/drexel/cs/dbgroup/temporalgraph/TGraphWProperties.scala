package edu.drexel.cs.dbgroup.temporalgraph

import java.util.Map
import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import java.time.LocalDate

import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.{HashPartitioner,RangePartitioner}

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap

import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps

abstract class TGraphWProperties(spec: GraphSpec, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraph[VertexEdgeAttribute, VertexEdgeAttribute] {

  val storageLevel = storLevel
  //whether this TGraph is known to be coalesced
  //false means it may or may not be coalesced
  //whereas true means definitely coalesced
  val coalesced: Boolean = coal

  //in order to better support maps and other operations on properties
  //we maintain a schema for the graph which is a type spec for each possible property
  //this does not mean each property is contained in each vertex or edge
  val graphSpec: GraphSpec = spec

  protected def aggregateByChange(c: ChangeSpec, vgroupby: (VertexId, VertexEdgeAttribute) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VertexEdgeAttribute, VertexEdgeAttribute) => VertexEdgeAttribute, eAggFunc: (VertexEdgeAttribute, VertexEdgeAttribute) => VertexEdgeAttribute): TGraphWProperties
  protected def aggregateByTime(c: TimeSpec, vgroupby: (VertexId, VertexEdgeAttribute) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VertexEdgeAttribute, VertexEdgeAttribute) => VertexEdgeAttribute, eAggFunc: (VertexEdgeAttribute, VertexEdgeAttribute) => VertexEdgeAttribute): TGraphWProperties

  /**
    * Transforms the attributes of the graph
    * @param emap The mapping function for edges
    * @param vmap The mapping function for vertices
    * @param newSpec The new graph specification/schema
    * @return tgraph The transformed graph. The temporal schema is unchanged.

  def map(emap: Edge[VertexEdgeAttribute] => VertexEdgeAttribute, vmap: (VertexId, VertexEdgeAttribute) => VertexEdgeAttribute, newSpec: GraphSpec): TGraphWProperties
    */
  /**
    * Transforms each vertex attribute in the graph for each time period
    * using the map function. 
    *
    * @param map the function from a vertex object to a new vertex value
    *
    */
  def vmap(map: (VertexId, Interval, VertexEdgeAttribute) => VertexEdgeAttribute, newSpec: GraphSpec): TGraphWProperties

  /**
   * Transforms each edge attribute in the graph using the map function.  The map function is not
   * passed the vertex value for the vertices adjacent to the edge.  If vertex values are desired,
   * use `mapTriplets`.

   * @param map the function from an edge object with a time index to a new edge value.
   *
   */
  def emap(map: (Interval, Edge[VertexEdgeAttribute]) => VertexEdgeAttribute, newSpec: GraphSpec): TGraphWProperties

  /**
    * Produce a union of two temporal graphs. 
    * @param other The other TGraph
    * @return new TGraph with the union of entities from both graphs within each chronon.
    */
  def union(other: TGraphWProperties): TGraphWProperties

  /**
    * Produce the intersection of two temporal graphs.
    * @param other The other TGraph
    * @return new TemporaGraph with the intersection of entities from both graphs within each chronon.
    */
  def intersection(other: TGraphWProperties): TGraphWProperties

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
    * @return New graph with pageranks for each interval
    */
  def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): TGraphWProperties

  /**
   * Run connected components algorithm on a temporal graph
   * return a graph with the vertex value containing the lowest vertex
   * id in the connected component containing that vertex.
   *
   * @return New graph with vertex attribute the id of 
   * the smallest vertex in each connected component 
   * for Intervals in which the vertex appears
   */
  def connectedComponents(): TGraphWProperties
  
  /**
   * Computes shortest paths to the given set of landmark vertices.
   * @param landmarks the list of landmark vertex ids to which shortest paths will be computed
    * @param uni Treat the graph as undirected or directed. true = directed
    * @return Graph with vertices where each vertex attribute
   * is the shortest-path distance to each reachable landmark vertex.
   */
  def shortestPaths(uni: Boolean, landmarks: Seq[VertexId]): TGraphWProperties

  protected def emptyGraph: TGraphWProperties

}

object TGraphWProperties {
  def computeIntervals(verts: RDD[(VertexId, Interval)], edgs: RDD[((VertexId, VertexId), Interval)], vprops: RDD[(VertexId, (Interval, VertexEdgeAttribute))], eprops: RDD[((VertexId, VertexId), (Interval, VertexEdgeAttribute))]): RDD[Interval] = {
    val dates: RDD[LocalDate] = verts.flatMap{ case (id, intv) => List(intv.start, intv.end)}.union(edgs.flatMap{ case (ids, intv) => List(intv.start, intv.end)}).union(vprops.flatMap{ case (id, (intv, attr)) => List(intv.start, intv.end)}).union(eprops.flatMap{ case (ids, (intv, attr)) => List(intv.start, intv.end)}).distinct
    implicit val ord = TempGraphOps.dateOrdering
    dates.sortBy(c => c, true, 1).sliding(2).map(lst => Interval(lst(0), lst(1)))
  }

  /*
   * given an RDD where for a key there is a value over time period
   * coalesce consecutive periods
   * i.e. (1, 1-3) and (1, 3-4) become
   * single tuple (1, 1-4)
   * Warning: This is a very expensive operation, use sparingly
   */
  //TODO: there are 3 different version of coalesce between this and TGraphNoSchema
  //refactor
  def coalesce[K: Ordering : ClassTag](rdd: RDD[(K, Interval)]): RDD[(K, Interval)] = {
    implicit val ord = TempGraphOps.dateOrdering
    //it's faster if we partition first
    //TODO: in some cases RangePartition provides better performance, but in others it makes it too slow
    //rdd.partitionBy(new org.apache.spark.RangePartitioner(rdd.getNumPartitions, rdd))
    rdd.partitionBy(new HashPartitioner(rdd.getNumPartitions))
      .groupByKey.mapValues{ seq =>  //groupbykey produces RDD[(K, Seq[V])]
      seq.toSeq.sortBy(x => x.start)
        .foldLeft(List[Interval]()){ (r,c) => r match {
          case head :: tail =>
            if (head.end == c.start || head.intersects(c)) Interval(head.start, c.end) :: tail
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
  //TODO: there are three diff versions of constrain between this and TGraphNoSchema
  //refactor
  def constrainEdges(verts: RDD[(VertexId, Interval)], edgs: RDD[((VertexId, VertexId), Interval)]): RDD[((VertexId, VertexId), Interval)] = {
    if (verts.isEmpty) return ProgramContext.sc.emptyRDD[((VertexId, VertexId), Interval)]

    //if we don't have many vertices, we can do a better job with broadcasts
    //FIXME: what should this number be? it should depend on memory size
    //TODO: pull this logic into query optimization or use DataFrames so that sql can do it automatically
    if (verts.countApprox(2).getFinalValue.high < 10000000) {
      //coalesce before collect
      //TODO: use future so that this broadcast only happens if we need edges
      val bverts = ProgramContext.sc.broadcast(verts.aggregateByKey(List[Interval]())(seqOp = (u,v) => v :: u, combOp = (u1,u2) => u1 ++ u2).mapValues(_.sorted.foldLeft(List[Interval]()){ (r,c) => r match {
        case head :: tail =>
          if (head.end == c.start || head.intersects(c)) Interval(head.start, c.end) :: tail
          else c :: r
        case Nil => List(c)
      }}).collectAsMap())

      edgs.mapPartitions({ iter =>
        val m: scala.collection.Map[VertexId, List[Interval]] = bverts.value
        for {
          ((vid1, vid2), p) <- iter
          l1 = m.get(vid1).getOrElse(List[Interval]()).filter(ii => ii.intersects(p))
          l2 = m.get(vid2).getOrElse(List[Interval]()).filter(ii => ii.intersects(p))
          if l1.size == 1 && l2.size == 1 && l1.head.intersects(l2.head)
        } yield ((vid1, vid2), Interval(TempGraphOps.maxDate(p.start, m(vid1).find(_.intersects(p)).get.start, m(vid2).find(_.intersects(p)).get.start), TempGraphOps.minDate(p.end, m(vid1).find(_.intersects(p)).get.end, m(vid2).find(_.intersects(p)).get.end)))
      }, preservesPartitioning = true)

    } else {
      //coalesce structure first so that edges are not split up
      val coalescV = TGraphWProperties.coalesce(verts).partitionBy(new HashPartitioner(edgs.getNumPartitions))

      //get edges that are valid for each of their two vertices
      edgs.map(e => (e._1._1, e))
        .join(coalescV) //this creates RDD[(VertexId, (((VertexId, VertexId), Interval), Interval))]
        .filter{case (vid, (e, v)) => e._2.intersects(v) } //this keeps only matches of vertices and edges where periods overlap
        .map{case (vid, (e, v)) => (e._1._2, (e, v))}      
        .join(coalescV) //this creates RDD[(VertexId, ((((VertexId, VertexId), Interval), Interval), Interval)
        .filter{ case (vid, (e, v)) => e._1._2.intersects(v) && e._2.intersects(v)}
        .map{ case (vid, (e, v)) => (e._1._1, Interval(TempGraphOps.maxDate(v.start, e._1._2.start, e._2.start), TempGraphOps.minDate(v.end, e._1._2.end, e._2.end)))}
    }
  }

  /*
   * Given an RDD of vertices and an RDD of edge properties,
   * remove the edges for which there is no src or dst vertex
   * at the indicated time period,
   * shorten the periods as needed to meet the integrity constraint.
   * Note: verts is expected to be coalesced
   * Warning: This is a very expensive operation, use only when needed.
   */
  def constrainEProperties(verts: RDD[(VertexId, Interval)], edgs: RDD[((VertexId, VertexId), (Interval, VertexEdgeAttribute))]): RDD[((VertexId, VertexId), (Interval, VertexEdgeAttribute))] = {
    if (verts.isEmpty) return ProgramContext.sc.emptyRDD[((VertexId, VertexId), (Interval, VertexEdgeAttribute))]

    //if we don't have many vertices, we can do a better job with broadcasts
    //FIXME: what should this number be? it should depend on memory size
    //TODO: pull this logic into query optimization or use DataFrames so that sql can do it automatically
    if (verts.countApprox(2).getFinalValue.high < 10000000) {
      //TODO: use future so that this broadcast only happens if we need edges
      val bverts = ProgramContext.sc.broadcast(verts.aggregateByKey(List[Interval]())(seqOp = (u,v) => v :: u, combOp = (u1,u2) => u1 ++ u2).collectAsMap())

      edgs.mapPartitions({ iter =>
        val m: scala.collection.Map[VertexId, List[Interval]] = bverts.value
        for {
          ((vid1, vid2), (p, v)) <- iter
          l1 = m.get(vid1).getOrElse(List[Interval]()).filter(ii => ii.intersects(p))
          l2 = m.get(vid2).getOrElse(List[Interval]()).filter(ii => ii.intersects(p))
          if l1.size == 1 && l2.size == 1 && l1.head.intersects(l2.head)
        } yield ((vid1, vid2), (Interval(TempGraphOps.maxDate(p.start, m(vid1).find(_.intersects(p)).get.start, m(vid2).find(_.intersects(p)).get.start), TempGraphOps.minDate(p.end, m(vid1).find(_.intersects(p)).get.end, m(vid2).find(_.intersects(p)).get.end)), v))
      }, preservesPartitioning = true)

    } else {
      val coalescV = verts.partitionBy(new HashPartitioner(edgs.getNumPartitions))

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
