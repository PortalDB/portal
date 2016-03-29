package edu.drexel.cs.dbgroup.temporalgraph

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

abstract class TemporalGraph[VD: ClassTag, ED: ClassTag] extends Serializable {

  /**
    * The length of time covered by each time period in the temporal sequence of this temporal graph
    */
  def resolution(): Resolution

  /**
    * The size of the temporal sequence and also the number of snapshot graphs
    */
  def size(): Int

  /**
    *  The call to materialize the data structure
    */
  def materialize(): Unit

  /**
    * An RDD containing the vertices and their associated attributes.
    * @return an RDD containing the vertices in this graph, across all intervals.
    * The vertex attributes are in a Map of TimeIndex->value.
    */
  def vertices: VertexRDD[Map[Interval, VD]]

  /**
    * An RDD containing the vertices and their associated attributes.
    * @return an RDD containing the vertices in this graph, across all intervals.
    * The vertex attributes are a tuple of (TimeIndex, value),
    * which means that if the same vertex appears in multiple snapshots/time instances,
    * it will appear multiple times in the RDD.
    */
  def verticesFlat: VertexRDD[(Interval, VD)]

  /**
    * An RDD containing the edges and their associated attributes.
    * @return an RDD containing the edges in this graph, across all intervals.
    */
  def edges: EdgeRDD[Map[Interval, ED]]
  def edgesFlat: EdgeRDD[(Interval, ED)]

  /**
    * The degree of each vertex in the graph for each time index.
    * @note Vertices with no edges are not returned in the resulting RDD.
    */
  def degrees: VertexRDD[Map[Interval, Int]]

  /**
    * Get the temporal sequence for this graph.
    */
  def getTemporalSequence: Seq[Interval]

  /**
    * Get a snapshot for an interval
    * if the interval is invalid, an empty graph is returned
    */
  def getSnapshot(period: Interval):Graph[VD,ED]

  /**
    * Query operations
    */

  /**
    * Select a temporal subset of the graph. T-Select.
    * @param bound Time period to extract. 
    * @return temporalgraph with the temporal intersection or empty graph if 
    * the requested interval is wholely outside of the graph bounds.
    */
  def select(bound: Interval): TemporalGraph[VD, ED]

  /**
    * Select a temporal subset of the graph based on a temporal predicate.
    * @param tpred Time predicate to evalute for each time period of the temporal sequence.
    * @return temporalgraph with the temporal intersection. The structural schema of the graph is not affected.
    */
  def select(tpred: Interval => Boolean): TemporalGraph[VD, ED]

  /**
    * Restrict the graph to only the vertices and edges that satisfy the predicates. S-Select.
    * @param epred The edge predicate, which takes a triplet and evaluates to true 
    * if the edge is to be included.
    * @param vpred The vertex predicate, which takes a vertex object and evaluates 
    * to true if the vertex is to be included.
    * @return The temporal subgraph containing only the vertices and edges that satisfy 
    * the predicates. If no vertices/edges satisfy the predicates in a snapshot, that 
    * snapshot is empty but still included, i.e. the temporal schema of the graph is not changed.
    */
  def select(epred: EdgeTriplet[VD,ED] => Boolean, vpred: (VertexId, VD) => Boolean): TemporalGraph[VD, ED]

  /**
    * Create an aggregate graph over the whole time period.
    * @param resolution The desired duration of time intervals in the temporal sequence in sematic units (days, months, years)
    * @param semantics The AggregateSemantics type
    * @param vAggFunction The function to apply to vertex attributes during aggregation.
    * Any associative function can be supported, since the attribute aggregation is
    * performed in pairs (ala reduce).
    * @param eAggFunction The function to apply to edge attributes during aggregation.
    * Any associative function can be supported, since the attribute aggregation is
    * performed in pairs (ala reduce).
    * @return New temporal graph with specified resolution. Note that
    * some of the return snapshots may be empty as a result of an aggregation.
    * @throws IllegalArgumentException if the graphs are not union-compatible.
    */
  @throws(classOf[IllegalArgumentException])
  def aggregate(res: Resolution, vsem: AggregateSemantics.Value, esem: AggregateSemantics.Value, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TemporalGraph[VD, ED]

  /**
    * Transforms the structural schema of the graph
    * @param emap The mapping function for edges
    * @param vmap The mapping function for vertices
    * @return tgraph The transformed graph. The temporal schema is unchanged.
    */
  def transform[ED2: ClassTag, VD2: ClassTag](emap: (Edge[ED], Interval) => ED2, vmap: (VertexId, Interval, VD) => VD2): TemporalGraph[VD2, ED2]

  /**
    * Transforms each vertex attribute in the graph for each time period
    * using the map function. 
    * Special case of general transform, included here for better compatibility with GraphX.
    *
    * @param map the function from a vertex object to a new vertex value
    *
    * @tparam VD2 the new vertex data type
    *
    */
  def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2)
    (implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED]

  /**
    * Like above, but uses 0-based time indices instead of intervals,
    * which is faster where indices suffice.
    */
  def mapVerticesWIndex[VD2: ClassTag](map: (VertexId, TimeIndex, VD) => VD2)
    (implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED]

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
  def mapEdges[ED2: ClassTag](map: (Edge[ED], Interval) => ED2): TemporalGraph[VD, ED2]

  /**
    * Like above, but uses 0-based time indices instead of intervals,
    * which is faster where indices suffice.
    */
  def mapEdgesWIndex[ED2: ClassTag](map: (Edge[ED], TimeIndex) => ED2): TemporalGraph[VD, ED2]

  /**
   * Joins the vertices with entries in the `table` RDD and merges the results using `mapFunc`.
   * The input table should contain at most one entry for each vertex per time slice.  If no entry in `other` is
   * provided for a particular vertex in the graph, the map function receives `None`.
   *
   * @tparam U the type of entry in the table of updates
   * @tparam VD2 the new vertex value type
   *
   * @param other the table to join with the vertices in the graph.
   *              The table should contain at most one entry for each vertex.
   * @param mapFunc the function used to compute the new vertex values.
   *                The map function is invoked for all vertices, even those
   *                that do not have a corresponding entry in the table.
   *
   * @example This function is used to update the vertices with new values based on external data.
   */
  def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, Map[Interval, U])])
  (mapFunc: (VertexId, Interval, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null)
      : TemporalGraph[VD2, ED]

  /**
    * Produce a union of two union-compatible temporal graphs. 
    * The result is a union of the temporal sequences of the two graphs.
    * For the snapshots that correspond to the same time period,
    * the any(existential) or all(universal) semantics is applied.
    * If there are any vertices or edges in the
    * overlapping snapshot that themselves overlap (have the same id), 
    * the transformation function vFunc/eFunc is applied.
    * @param other The other TemporalGraph with the same structural schema
    * @param sem All or Any semantics
    * @param vFunc The combination function when the same vertex is found within the same time period
    * @param eFunc The combination function when the same edge is found within the same time period
    * @return new TemporalGraph with the union of snapshots from both graphs
    * @throws IllegalArgumentException if the graphs are not union-compatible.
    */
  @throws(classOf[IllegalArgumentException])
  def union(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED]

  /**
    * Produce the intersection of two union-compatible temporal graphs.
    * The result is an intersection of the temporal sequences of the two graphs.
    * For the snapshots that correspond to the same time period,
    * the any(existential) or all(universal) semantics is applied.
    * If there are any vertices or edges in the overlapping snapshot that
    * themselves overlap (have the same id),
    * the transformation function vFunc/eFunc is applied.
    * @param other The other TemporalGraph with the same structural schema
    * @param sem All or Any semantics
    * @param vFunc The combination function when the same vertex is found within the same time period
    * @param eFunc The combination function when the same edge is found within the same time period
    * @return new TemporaGraph with the intersection of snapshots from both graphs
    * @throws IllegalArgumentException if the graphs are not union-compatible
    */
  @throws(classOf[IllegalArgumentException])
  def intersect(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED]

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
       mergeMsg: (A, A) => A): TemporalGraph[VD, ED]

  /**
    * Run pagerank on all intervals. It is up to the implementation to run sequantially,
    * in parallel, incrementally, etc. The number of iterations will depend on both
    * the numIter argument and the rate of convergence, whichever occurs first.
    * @param uni Treat the graph as undirected or directed. true = undirected
    * @param tol epsilon, measure of convergence
    * @param resetProb probability of reset/jump
    * @param numIter number of iterations of the algorithm to run. If omitted, will run
    * until convergence of the tol argument.
    * @return new Temporal Graph with pagerank as vertex attribute
    */
  def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): TemporalGraph[Double,Double]

  /**
   * Run connected components algorithm on a temporal graph
   * return a graph with the vertex value containing the lowest vertex
   * id in the connected component containing that vertex.
   *
   * @return a new Temporal Graph in which each vertex attribute is a list of
   * the smallest vertex in each connected component for Intervals in which the vertex appears
   */
  def connectedComponents(): TemporalGraph[VertexId, ED]
  
  /**
   * Computes shortest paths to the given set of landmark vertices.
   * @param landmarks the list of landmark vertex ids to which shortest paths will be computed 
   *
   * @return a new Temporal Graph where each vertex attribute is the shortest-path distance to
   * each reachable landmark vertex.
   */
  def shortestPaths(landmarks: Seq[VertexId]): TemporalGraph[Map[VertexId, Int], ED]

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
  def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): TemporalGraph[VD, ED]

  /**
   * Uncaches both vertices and edges of this graph. This is useful in iterative algorithms that
   * build a new graph in each iteration.
   */
  def unpersist(blocking: Boolean = true): TemporalGraph[VD, ED]

  /**
    * Repartition the edges in the graph according to the strategy.
    * The number of partitions remains the same as prior to this operation.
    * @param partitionStrategy the partitioning strategy type to use. The strategy
    * object itself is created by the PartitionStrategies.makeStrategy factory call.
    * @param runs The width of the run (only applicable for hybrid strategies, 
    * otherwise ignored.
    * @return new partitioned graph
    */
  def partitionBy(pst: PartitionStrategyType.Value, runs: Int): TemporalGraph[VD, ED]

  /**
    * Repartition the edges in the graph according to the strategy.
    * @param partitionStrategy the partitioning strategy type to use. The strategy
    * object itself is created by the PartitionStrategies.makeStrategy factory call.
    * @param runs The width of the run (only applicable for hybrid strategies, 
    * otherwise ignored.
    * @param parts The number of partitions to partition into.
    * @return new partitioned graph
    */
  def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): TemporalGraph[VD, ED]

}
