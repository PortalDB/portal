package edu.drexel.cs.dbgroup.temporalgraph

import java.util.Map

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps._

import java.time.LocalDate

//TODO: this really should be a trait but traits do not allow type parameters
//so this would require defining implicit evidence, etc.
abstract class TGraph[VD: ClassTag, ED: ClassTag] extends Serializable {

  /**
    * The duration the temporal sequence
    */
  def size(): Interval

  /**
    *  The call to materialize the data structure
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
    * We are returning RDD rather than VertexRDD because VertexRDD
    * cannot have duplicates for vid.
    */
  def vertices: RDD[(VertexId,(Interval, VD))]

  /**
    * An RDD containing the vertices and their associated attributes.
    * @return an RDD containing the vertices in this graph, across all intervals.
    * The vertex attributes are in a Map of Interval->value.
    * The interval is maximal.
    */
  def verticesAggregated: RDD[(VertexId,Map[Interval, VD])]

  /**
    * An RDD containing the edges and their associated attributes.
    * @return an RDD containing the edges in this graph, across all intervals.
    */
  def edges: RDD[((VertexId,VertexId),(Interval, ED))]

  /**
    * An RDD containing the edges and their associated attributes.
    * @return an RDD containing the edges in this graph, across all intervals.
    * The edge attributes are in a Map of Interval->value.
    * The interval is maximal.
    */
  def edgesAggregated: RDD[((VertexId,VertexId),Map[Interval, ED])]

  /**
    * Get the temporal sequence for the representative graphs
    * composing this tgraph. Intervals are consecutive but
    * not equally sized.
    */
  def getTemporalSequence: RDD[Interval]

  /**
    * Get a snapshot for a point in time
    * if the time is outside the graph bounds, an empty graph is returned
    */
  def getSnapshot(time: LocalDate):Graph[VD,ED]

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
    * Select a temporal subset of the graph. T-Select.
    * @param bound Time period to extract. 
    * @return temporalgraph with the temporal intersection or empty graph if 
    * the requested interval is wholely outside of the graph bounds.
    */
  def slice(bound: Interval): TGraph[VD, ED]

  /**
    * Select a subgraph based on the vertex attributes.
    * @param vpred Vertex predicate.
    * Foreign key constraint is enforced.
    */
  def vsubgraph(vpred: (VertexId, VD, Interval ) => Boolean ): TGraph[VD,ED]


  /**
    * Select a subgraph based on the edge attributes.
    * @param epred Edge predicate.
    * Foreign key constraint is enforced.
    */
  def esubgraph(epred: (EdgeTriplet[VD,ED],Interval  ) => Boolean): TGraph[VD,ED]


  /**
    * Create a temporal node
    * @param window The desired duration of time intervals in the temporal sequence in sematic units (days, months, years) or number of changes
    * @param vquant The quantification over vertices
    * @param equant The quantification over edges
    * @param vAggFunc The function to apply to vertex attributes during aggregation.
    * Any associative function can be supported, since the attribute aggregation is
    * performed in pairs (ala reduce).
    * @param eAggFunct The function to apply to edge attributes during aggregation.
    * Any associative function can be supported, since the attribute aggregation is
    * performed in pairs (ala reduce).
    * @return New tgraph 
    */

  def createTemporalNodes(window: WindowSpecification, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraph[VD, ED]

  /**
    * create a attribute node
    * @param vgroupby The grouping function for vertices for structural aggregation
    * @param vAggFunc The function to apply to vertex attributes during aggregation.
    * Any associative function can be supported, since the attribute aggregation is
    * performed in pairs (ala reduce).
    * @param eAggFunc The function to apply to edge attributes during aggregation.
    * Any associative function can be supported, since the attribute aggregation is
    * performed in pairs (ala reduce).
    * @return New tgraph
    */
  def createAttributeNodes( vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED)(vgroupby: (VertexId, VD) => VertexId ): TGraph[VD, ED]

  /**
    * The analytics methods
    */

  /**
    * Execute a Pregel-like iterative vertex-parallel abstraction.  The
    * user-defined vertex-program `vprog` is executed in parallel on
    * each vertex receiving any inbound messages and computing a new
    * value for the vertex.  The `sendMsg` function is then invoked on
    * all out-edges and is used to compute an optional message to the
    * destination vertex. The `mergeMsg` function is a commutative
    * associative function used to combine messages destined to the
    * same vertex. The computation is performed on all time periods consecutively
    * or all together, depending on the implementation of the graph.
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
       sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
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
   * Caches the vertices and edges associated with this graph at the specified storage level,
   * ignoring any target storage levels previously set.
   *
   * @param newLevel the level at which to cache the graph.
   *
   * @return A reference to this graph for convenience.
   */
  def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): TGraph[VD, ED]

  /**
   * Uncaches both vertices and edges of this graph. This is useful in iterative algorithms that
   * build a new graph in each iteration.
   */
  def unpersist(blocking: Boolean = true): TGraph[VD, ED]

  /**
    * Repartition the edges in the graph according to the strategy.
    * @param partitionStrategy the partitioning strategy type to use. The strategy
    * object itself is created by the PartitionStrategies.makeStrategy factory call.
    * @param tgp The partitioning object including strategy, runs, and number of partitions.
    * @return new partitioned graph
    */
  def partitionBy(tgp: TGraphPartitioning): TGraph[VD, ED]

}
