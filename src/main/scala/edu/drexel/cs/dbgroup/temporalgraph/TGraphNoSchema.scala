package edu.drexel.cs.dbgroup.temporalgraph

import scala.reflect.ClassTag

import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.rdd.RDD

import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps._

import java.time.LocalDate

abstract class TGraphNoSchema[VD: ClassTag, ED: ClassTag](verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[((VertexId, VertexId), (Interval, ED))]) extends TGraph[VD, ED] {

  val allVertices: RDD[(VertexId, (Interval, VD))] = verts
  val allEdges: RDD[((VertexId, VertexId), (Interval, ED))] = edgs

  lazy val intervals: Seq[Interval] = {
    val dates: RDD[LocalDate] = verts.flatMap{ case (id, (intv, attr)) => List(intv.start, intv.end)}.union(edgs.flatMap { case (ids, (intv, attr)) => List(intv.start, intv.end)}).distinct
    dates.sortBy(c => c, true).collect.sliding(2).map(lst => Interval(lst(0), lst(1))).toSeq
  }

  lazy val span: Interval = if (intervals.size > 0) Interval(intervals.head.start, intervals.last.end) else Interval(LocalDate.now, LocalDate.now)

  /**
    * The duration the temporal sequence
    */
  override def size(): Interval = span

  /**
    * An RDD containing the vertices and their associated attributes.
    * @return an RDD containing the vertices in this graph, across all intervals.
    * The vertex attributes are in a Map of Interval->value.
    * The interval is maximal.
    */
  override def vertices: RDD[(VertexId,Map[Interval, VD])] = {
    allVertices.mapValues(y => Map[Interval, VD](y._1 -> y._2))
      .reduceByKey((a: Map[Interval, VD], b: Map[Interval, VD]) => a ++ b)
  }

  /**
    * An RDD containing the vertices and their associated attributes.
    * @return an RDD containing the vertices in this graph, across all intervals.
    * The vertex attributes are a tuple of (Interval, value),
    * which means that if the same vertex appears in multiple periods,
    * it will appear multiple times in the RDD.
    * The interval is maximal.
    * We are returning RDD rather than VertexRDD because VertexRDD
    * cannot have duplicates for vid.
    */
  def verticesFlat: RDD[(VertexId,(Interval, VD))] = allVertices

  /**
    * An RDD containing the edges and their associated attributes.
    * @return an RDD containing the edges in this graph, across all intervals.
    */
  override def edges: RDD[((VertexId,VertexId),Map[Interval, ED])] = {
    allEdges.mapValues(y => Map[Interval, ED](y._1 -> y._2))
      .reduceByKey((a: Map[Interval, ED], b: Map[Interval, ED]) => a ++ b)
  }

  def edgesFlat: RDD[((VertexId,VertexId),(Interval, ED))] = allEdges

  /**
    * Get the temporal sequence for the representative graphs
    * composing this tgraph. Intervals are consecutive but
    * not equally sized.
    */
  override def getTemporalSequence: Seq[Interval] = intervals

  /**
    * Restrict the graph to only the vertices and edges that satisfy the predicates.
    * @param epred The edge predicate, which takes an edge and evaluates to true 
    * if the edge is to be included.
    * @param vpred The vertex predicate, which takes a vertex object and evaluates 
    * to true if the vertex is to be included.
    * This is the most general version of select.
    * @return The temporal subgraph containing only the vertices and edges 
    * that satisfy the predicates. The result is coalesced which
    * may cause different representative intervals.
    */
  def select(epred: ((VertexId, VertexId), (Interval, ED)) => Boolean, vpred: (VertexId, (Interval, VD)) => Boolean): TGraph[VD, ED]

  /** Spark-specific */

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): TGraphNoSchema[VD, ED] = {
    allVertices.persist(newLevel)
    allEdges.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): TGraphNoSchema[VD, ED] = {
    allVertices.unpersist(blocking)
    allEdges.unpersist(blocking)
    this
  }



  /** Utility methods **/

  /*
   * given an RDD where for a key there is a value over time period
   * coalesce consecutive periods with the same value
   * i.e. (1, (1-3, "blah")) and (1, (3-4, "blah")) become
   * single tuple (1, (1-4, "blah"))
   * Warning: This is a very expensive operation, use sparingly
   */
  protected def coalesce[K: ClassTag, V: ClassTag](rdd: RDD[(K, (Interval, V))]): RDD[(K, (Interval, V))] = {
    rdd.groupByKey.mapValues{ seq =>  //groupbykey produces RDD[(K, Seq[(p, V)])]
      seq.toSeq.sortBy(x => x._1.start)
      //TODO: see if using list and :: is faster
        .foldLeft(Seq[(Interval, V)]()){ (r,c) => r match {
          case head :+ last => if (last._2 == c._2 && last._1.end == c._1.start) head :+ (Interval(last._1.start, c._1.end), last._2) else r :+ c
          case Nil => Seq(c)
        }}
    }.flatMap{ case (k,v) => v.map(x => (k, x))}
  }

  /*
   * given an RDD where for a key there is a value over time period
   * coalesce consecutive periods for the same keys regardless of values
   * i.e. (1, (1-3, "blah")) and (1, (3-4, "bob")) become
   * single tuple (1, 1-4)
   * Warning: This is a very expensive operation, use sparingly
   */
  protected def coalesceStructure[K: ClassTag, V: ClassTag](rdd: RDD[(K, (Interval, V))]): RDD[(K, Interval)] = {
    rdd.groupByKey.mapValues{ seq =>  //groupbykey produces RDD[(K, Seq[(p, V)])]
      seq.toSeq.sortBy(x => x._1.start)
      //TODO: see if using list and :: is faster
        .foldLeft(Seq[(Interval, V)]()){ (r,c) => r match {
          case head :+ last => if (last._1.end == c._1.start) head :+ (Interval(last._1.start, c._1.end), last._2) else r :+ c
          case Nil => Seq(c)
        }}
    }.flatMap{ case (k,v) => v.map(x => (k, x._1))}
  }

}
