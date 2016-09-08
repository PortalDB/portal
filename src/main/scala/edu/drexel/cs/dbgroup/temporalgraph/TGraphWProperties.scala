package edu.drexel.cs.dbgroup.temporalgraph

import java.util.Map
import scala.collection.JavaConversions._

import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.rdd.RDD

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap

abstract class TGraphWProperties(storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraph[VertexEdgeAttribute, VertexEdgeAttribute] {

  val storageLevel = storLevel
  //whether this TGraph is known to be coalesced
  //false means it may or may not be coalesced
  //whereas true means definitely coalesced
  val coalesced: Boolean = coal

  //TODO: add some notion of schema?

  /**
    * An RDD containing the vertices and their associated attributes.
    * @return an RDD containing the vertices in this graph, across all intervals.
    * The vertex attributes are in a Map of Interval->value.
    * The interval is maximal.
    */
  override def verticesAggregated: RDD[(VertexId,Map[Interval, VertexEdgeAttribute])] = {
    vertices.mapValues(y => {var tmp = new Object2ObjectOpenHashMap[Interval,VertexEdgeAttribute](); tmp.put(y._1, y._2); tmp.asInstanceOf[Map[Interval, VertexEdgeAttribute]]})
      .reduceByKey((a: Map[Interval, VertexEdgeAttribute], b: Map[Interval, VertexEdgeAttribute]) => a ++ b)
  }

  /**
    * An RDD containing the edges and their associated attributes.
    * @return an RDD containing the edges in this graph, across all intervals.
    */
  override def edgesAggregated: RDD[((VertexId,VertexId),Map[Interval, VertexEdgeAttribute])] = {
    edges.mapValues(y => {var tmp = new Object2ObjectOpenHashMap[Interval,VertexEdgeAttribute](); tmp.put(y._1, y._2); tmp.asInstanceOf[Map[Interval, VertexEdgeAttribute]]})
      .reduceByKey((a: Map[Interval, VertexEdgeAttribute], b: Map[Interval, VertexEdgeAttribute]) => a ++ b)
  }

  override def aggregate(res: WindowSpecification, vquant: Quantification, equant: Quantification, vAggFunc: (VertexEdgeAttribute, VertexEdgeAttribute) => VertexEdgeAttribute, eAggFunc: (VertexEdgeAttribute, VertexEdgeAttribute) => VertexEdgeAttribute)(vgroupby: (VertexId, VertexEdgeAttribute) => VertexId = vgb): TGraphWProperties = {
    //aggregateByChange requires coalesced tgraph for correctness
    //both produce potentially uncoalesced TGraph
    res match {
      case c : ChangeSpec => coalesce().asInstanceOf[TGraphWProperties].aggregateByChange(c, vgroupby, vquant, equant, vAggFunc, eAggFunc)
      case t : TimeSpec => aggregateByTime(t, vgroupby, vquant, equant, vAggFunc, eAggFunc)
      case _ => throw new IllegalArgumentException("unsupported window specification")
    }
  }

  protected def aggregateByChange(c: ChangeSpec, vgroupby: (VertexId, VertexEdgeAttribute) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VertexEdgeAttribute, VertexEdgeAttribute) => VertexEdgeAttribute, eAggFunc: (VertexEdgeAttribute, VertexEdgeAttribute) => VertexEdgeAttribute): TGraphWProperties
  protected def aggregateByTime(c: TimeSpec, vgroupby: (VertexId, VertexEdgeAttribute) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VertexEdgeAttribute, VertexEdgeAttribute) => VertexEdgeAttribute, eAggFunc: (VertexEdgeAttribute, VertexEdgeAttribute) => VertexEdgeAttribute): TGraphWProperties

  /**
    * Transforms the attributes of the graph
    * @param emap The mapping function for edges
    * @param vmap The mapping function for vertices
    * @return tgraph The transformed graph. The temporal schema is unchanged.
    */
  def map(emap: Edge[VertexEdgeAttribute] => VertexEdgeAttribute, vmap: (VertexId, VertexEdgeAttribute) => VertexEdgeAttribute): TGraphWProperties

  /**
    * Transforms each vertex attribute in the graph for each time period
    * using the map function. 
    *
    * @param map the function from a vertex object to a new vertex value
    *
    */
  def mapVertices(map: (VertexId, Interval, VertexEdgeAttribute) => VertexEdgeAttribute): TGraphWProperties

  /**
   * Transforms each edge attribute in the graph using the map function.  The map function is not
   * passed the vertex value for the vertices adjacent to the edge.  If vertex values are desired,
   * use `mapTriplets`.

   * @param map the function from an edge object with a time index to a new edge value.
   *
   */
  def mapEdges(map: (Interval, Edge[VertexEdgeAttribute]) => VertexEdgeAttribute): TGraphWProperties

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
