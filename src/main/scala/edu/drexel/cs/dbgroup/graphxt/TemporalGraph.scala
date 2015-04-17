package edu.drexel.cs.dbgroup.graphxt

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

abstract class TemporalGraph[VD: ClassTag, ED: ClassTag] extends Serializable {

  /**
    * The number of non-temporal graphs/snapshots that compose this temporal graph
    */
  def size(): Int

  /**
    * The number of edges across all snapshots, which depends on the internal 
    * representation.
    */
  def numEdges(): Long

  /**
    * The number of partitions this graph occupies
    */
  def numPartitions(): Int

  /**
    * An RDD containing the vertices and their associated attributes.
    * @return an RDD containing the vertices in this graph, across all intervals.
    */
  def vertices: VertexRDD[VD]

  /**
    * An RDD containing the edges and their associated attributes.
    * @return an RDD containing the edges in this graph, across all intervals.
    */
  def edges: EdgeRDD[ED]

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
    * Add a snapshot to the graph for a single time period.
    * It is assumed that intervals are non-overlapping and at the same resolution.
    * @param place the interval of the snapshot to add
    * @param snap the snapshot graph to add
    * @return A new graph with the snapshot added
    */
  def addSnapshot(place: Interval, snap: Graph[VD, ED]): TemporalGraph[VD, ED]

  /**
    * Return the snapshot for a particular time period.
    * If the resolution of this graph is larger than 1, the snapshot of the period
    * which includes the requested time is returned.
    * @param time Time/number of the snapshot in question
    * @return graphx Graph of the snapshot or null if the requested time is 
    * absent in this temporal graph.
    */
  def getSnapshotByTime(time: Int): Graph[VD, ED]

  /**
    * Return the snapshot by index/temporal order which is relative to 
    * the time start of this specific graph.
    * @param position Index (zero-based) of the snapshot.
    * @return graphx Graph of the snapshot or null if the requested index
    * is outside of the graph bounds.
    */
  def getSnapshotByPosition(pos: Int): Graph[VD, ED]

  /**
    * Select a temporal subset of the graph.
    * @param bound Interval, inclusive, to extract. If the interval is exceeding
    * the bounds of the graph coverage, it is scaled down to graph bounds.
    * @return temporalgraph or null if the requested interval is wholely outside of 
    * the graph bounds.
    */
  def select(bound: Interval): TemporalGraph[VD, ED]

  /**
    * Create an aggregate graph over the whole time period.
    * @param resolution The number of consecutive graphs that compose a single 
    * aggregate. The total number of snapshots in the graph does not have to be 
    * evenly divisible by the resolution. The last aggregate will be composed 
    * from whatever snapshots remain.
    * @param semantics The AggregateSemantics type
    * @param vAggFunction The function to apply to vertex attributes during aggregation.
    * Any associative function can be supported, since the attribute aggregation is
    * performed in pairs (ala reduce).
    * @param eAggFunction The function to apply to edge attributes during aggregation.
    * Any associative function can be supported, since the attribute aggregation is
    * performed in pairs (ala reduce).
    * @return New temporal graph of size ceiling(currentsize/resolution). Note that
    * some of the return snapshots may be empty as a result of an aggregation.
    */
  def aggregate(resolution: Int, sem: AggregateSemantics.Value, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TemporalGraph[VD, ED]

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

}
