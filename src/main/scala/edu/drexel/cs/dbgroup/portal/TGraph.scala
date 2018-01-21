package edu.drexel.cs.dbgroup.portal

import java.util.Map

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

import edu.drexel.cs.dbgroup.portal.util.TempGraphOps._

import java.time.LocalDate

/**
  * A TGraph abstractly represents a single evolving graph, consisting
  * of nodes, node properties, edges, edge properties, and addition,
  * modification, and deletion of these over time.  The TGraph
  * provides operations of the temporal algebra and accessors.  Like
  * Spark and GraphX, the TGraph is immutable -- operations return new
  * TGraphs.
  * 
  * @note [[GraphLoader]] contains convenience operations for loading TGraphs
  * 
  * @tparam VD the node/vertex attribute type
  * @tparam ED the edge attribute type
  */
abstract class TGraph[VD: ClassTag, ED: ClassTag] extends Serializable {

  /** A friendly name for this TGraph */
  @transient var name: String = null

  /** Assign a name to this TGraph */
  def setName(_name: String): this.type = {
    name = _name
    this
  }

  /**
    * The duration the temporal sequence
    * @return an [[Interval]] from the earliest graph change to the last
    */
  def size(): Interval

  /**
    *  The call to materialize the data structure.
    *  This is a convenience operation for evaluation purposes.
    */
  def materialize(): Unit

  /**
    * An RDD containing the vertices and their associated attributes.
    * @return an RDD containing the vertices in this graph, across all intervals.
    * The vertex attributes are a tuple of (Interval, value),
    * which means that if the same vertex appears in multiple periods,
    * it will appear multiple times in the RDD.
    * The interval is maximal.
    * This is the direct match to our model.
    * @note An RDD is returned rather than VertexRDD because VertexRDD
    * cannot have duplicates for vid.
    */
  def vertices: RDD[(VertexId,(Interval, VD))]

  /**
    * An RDD containing the edges and their associated attributes.
    * @return an RDD containing the edges in this graph, across all intervals.
    */
  def edges: RDD[TEdge[ED]]

  /**
    * Get the temporal sequence for the representative graphs
    * composing this tgraph. 
    * @return RDD of [[Interval]]. Intervals are consecutive, nonoverlapping, and
    * not necessarily equally sized.
    */
  def getTemporalSequence: RDD[Interval]

  /**
    * Get a snapshot for a point in time.
    * @return Single GraphX [[org.apache.spark.graphx.Graph]].
    * @note If the time is outside the graph bounds, an empty graph is returned.
    * @note Unless in GraphX, each edge in TGraph has an [[EdgeId]].
    */
  def getSnapshot(time: LocalDate):Graph[VD,(EdgeId,ED)]

  /**
    * Query operations
    */

  /**
    * Coalesce the temporal graph.
    * Some operations are known to have the potential to make the TGraph uncoalesced.
    * If the graph is known to be coalesced, this is a no-op.
    */
  def coalesce(): TGraph[VD, ED]

  /**
    * Select a temporal subset of the graph.
    * @param bound Time period to extract. Intervals are closed-open. 
    * @return TGraph with the temporal intersection or empty graph if 
    * the requested interval is wholely outside of the graph bounds.
    */
  def slice(bound: Interval): TGraph[VD, ED]

  /**
    * Select a subgraph based on the vertex attributes.
    * @param vpred Vertex predicate. 
    * @return new TGraph, with only nodes that pass the predicate are retained.
    * Foreign key constraint is enforced.
    */
  def vsubgraph(pred: (VertexId, VD, Interval ) => Boolean ): TGraph[VD,ED]


  /**
    * Select a subgraph based on the edge attributes.
    * @param epred Edge predicate, with access to the attributes of both endpoint 
    * attributes and the edge attribute, as well as the time interval.
    * @return new TGraph, with only edges that pass the predicates, but all nodes,
    * even if those nodes have 0 degree as a result.
    */
  def esubgraph(pred: TEdgeTriplet[VD,ED] => Boolean, tripletFields: TripletFields = TripletFields.All): TGraph[VD,ED]


  /**
    * Time zoom
    * @param window The desired duration of time intervals in the temporal sequence in sematic units (days, months, years) or number of changes
    * @param vquant The quantification over vertices -- how much of the window must a node appear in to be included.
    * @param equant The quantification over edges -- how much of the window must an edge appear in to be included.
    * @param vAggFunc The function to apply to vertex attributes when multiple values exist in a time window.
    * Any associative function can be supported, since the attribute aggregation is
    * performed in pairs.
    * @param eAggFunct The function to apply to edge attributes when multiple values exist in a time widow.
    * Any associative function can be supported, since the attribute aggregation is
    * performed in pairs.
    * @return New tgraph with a modified temporal resolution. Foreign key constraint is enforced.
    */

  def createTemporalNodes(window: WindowSpecification, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraph[VD, ED]

  /**
    * Structural zoom. Creates nodes from clusters of existing nodes, assigning 
    * them new ids. Edge ids remain unchanged and are all preserved.
    * @param vgroupby The Skolem function that assigns a new node id consistently
    * @param vAggFunc The function to apply to node attributes when multiple nodes
    * are mapped by a Skolem function to a single group.
    * Any associative function can be supported, since the attribute aggregation is
    * performed in pairs (ala reduce).
    * @return New tgraph with only the new nodes and edges connecting them. A multigraph.
    */
  def createAttributeNodes( vAggFunc: (VD, VD) => VD)(vgroupby: (VertexId, VD) => VertexId ): TGraph[VD, ED]

  /**
    * The analytics methods
    */

  /**
    * Just as in GraphX, Execute a Pregel-like iterative
    * vertex-parallel abstraction, but over each time instance of the
    * TGraph.  The user-defined vertex-program `vprog` is executed in
    * parallel on each vertex receiving any inbound messages and
    * computing a new value for the vertex.  The `sendMsg` function is
    * then invoked on all out-edges and is used to compute an optional
    * message to the destination vertex. The `mergeMsg` function is a
    * commutative associative function used to combine messages
    * destined to the same vertex. The computation is performed on all
    * time periods consecutively or all together, depending on the
    * implementation of the graph.
    *
    * On the first iteration all vertices receive the `initialMsg` and
    * on subsequent iterations if a vertex does not receive a message
    * then the vertex-program is not invoked.
    *
    * This function iterates until there are no remaining messages, or
    * for `maxIterations` iterations.
    *
    * @tparam A the Pregel message type
    *
    * @param initialMsg the message each vertex will receive at the on
    * the first iteration
    *
    * @param maxIterations the maximum number of iterations to run for
    *
    * @param activeDirection the direction of edges incident to a vertex that received a message in
    * the previous round on which to run `sendMsg`. For example, if this is `EdgeDirection.Out`, only
    * out-edges of vertices that received a message in the previous round will run. The default is
    * `EdgeDirection.Either`, which will run `sendMsg` on edges where either side received a message
    * in the previous round. If this is `EdgeDirection.Both`, `sendMsg` will only run on edges where
    * *both* vertices received a message.
    *
    * @param vprog the user-defined vertex program which runs on each
    * vertex and receives the inbound message and computes a new vertex
    * value.  On the first iteration the vertex program is invoked on
    * all vertices and is passed the default message.  On subsequent
    * iterations the vertex program is only invoked on those vertices
    * that receive messages.
    *
    * @param sendMsg a user supplied function that is applied to out
    * edges of vertices that received messages in the current
    * iteration
    *
    * @param mergeMsg a user supplied function that takes two incoming
    * messages of type A and merges them into a single message of type
    * A.  ''This function must be commutative and associative and
    * ideally the size of A should not increase.''
    *
    * @return the resulting graph at the end of the computation
    *
    */
  def pregel[A: ClassTag]
     (initialMsg: A, defaultValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
       sendMsg: EdgeTriplet[VD, (EdgeId,ED)] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): TGraph[VD, ED]

  /**
    * The degree of each vertex in the graph by interval.
    * @note Vertices with no edges are not returned in the resulting RDD.
    */
  def degree(): RDD[(VertexId, (Interval, Int))]

  /**
    * The spark-specific partitioning-related methods
    */

  /**
    * The number of partitions this graph occupies
    */
  def numPartitions(): Int

  /**
   * Caches the vertices and edges associated with this graph at the
   * specified storage level, ignoring any target storage levels
   * previously set.
   *
   * @param newLevel the level at which to cache the graph.
   *
   * @return A reference to this graph for convenience.
   */
  def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): TGraph[VD, ED]

  /**
   * Uncaches both vertices and edges of this graph. This is useful in
   * iterative algorithms that build a new graph in each iteration.
   */
  def unpersist(blocking: Boolean = true): TGraph[VD, ED]

  /**
    * Repartition the edges in the graph according to the strategy.
    * @param tgp The partitioning object including strategy, runs, 
    * and number of partitions.
    * @return new partitioned graph
    */
  def partitionBy(tgp: TGraphPartitioning): TGraph[VD, ED]

}
