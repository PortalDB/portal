package edu.drexel.cs.dbgroup.temporalgraph

import scala.reflect.ClassTag

import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.HashPartitioner

import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps

import java.time.LocalDate

abstract class TGraphNoSchema[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[((VertexId, VertexId), (Interval, ED))], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY) extends TGraph[VD, ED] {

  val allVertices: RDD[(VertexId, (Interval, VD))] = verts
  val allEdges: RDD[((VertexId, VertexId), (Interval, ED))] = edgs
  val intervals: Seq[Interval] = intvs
  val storageLevel = storLevel
  val defaultValue: VD = defValue

  lazy val span: Interval = if (intervals.size > 0) Interval(intervals.head.start, intervals.last.end) else Interval(LocalDate.now, LocalDate.now)

  /**
    * The duration the temporal sequence
    */
  override def size(): Interval = span

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
  override def vertices: RDD[(VertexId,(Interval, VD))] = allVertices

  /**
    * An RDD containing the vertices and their associated attributes.
    * @return an RDD containing the vertices in this graph, across all intervals.
    * The vertex attributes are in a Map of Interval->value.
    * The interval is maximal.
    */
  override def verticesAggregated: RDD[(VertexId,Map[Interval, VD])] = {
    allVertices.mapValues(y => Map[Interval, VD](y._1 -> y._2))
      .reduceByKey((a: Map[Interval, VD], b: Map[Interval, VD]) => a ++ b)
  }

  override def edges: RDD[((VertexId,VertexId),(Interval, ED))] = allEdges

  /**
    * An RDD containing the edges and their associated attributes.
    * @return an RDD containing the edges in this graph, across all intervals.
    */
  override def edgesAggregated: RDD[((VertexId,VertexId),Map[Interval, ED])] = {
    allEdges.mapValues(y => Map[Interval, ED](y._1 -> y._2))
      .reduceByKey((a: Map[Interval, ED], b: Map[Interval, ED]) => a ++ b)
  }

  /**
    * Get the temporal sequence for the representative graphs
    * composing this tgraph. Intervals are consecutive but
    * not equally sized.
    */
  override def getTemporalSequence: Seq[Interval] = intervals

  override def getSnapshot(time: LocalDate): Graph[VD,ED] = {
    val index = intervals.indexWhere(a => a.contains(time))
    if (index >= 0) {
      val filteredvas: RDD[(VertexId,VD)] = allVertices.filter{ case (k,v) => v._1.contains(time)}
        .map{ case (k,v) => (k, v._2)}
      val filterededs: RDD[Edge[ED]] = allEdges.filter{ case (k,v) => v._1.contains(time)}.map{ case (k,v) => Edge(k._1, k._2, v._2)}
      Graph[VD,ED](filteredvas, filterededs, defaultValue, storageLevel, storageLevel)
    } else
      Graph[VD,ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
  }

  override def slice(bound: Interval): TGraphNoSchema[VD, ED] = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this

    if (!span.intersects(bound)) {
      return emptyGraph[VD,ED](defaultValue)
    }

    val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
    val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end
    val selectBound:Interval = Interval(startBound, endBound)

    fromRDDs(allVertices.filter{ case (vid, (intv, attr)) => intv.intersects(selectBound)}.mapValues(y => (Interval(TempGraphOps.maxDate(y._1.start, startBound), TempGraphOps.minDate(y._1.end, endBound)), y._2)), allEdges.filter{ case (vids, (intv, attr)) => intv.intersects(selectBound)}.mapValues(y => (Interval(TempGraphOps.maxDate(y._1.start, startBound), TempGraphOps.minDate(y._1.end, endBound)), y._2)), defaultValue, storageLevel)
  }

  override def select(vtpred: Interval => Boolean, etpred: Interval => Boolean): TGraphNoSchema[VD, ED] = {
    //because of the integrity constraint on edges, they have to 
    //satisfy both predicates
    fromRDDs(allVertices.filter{ case (vid, (intv, attr)) => vtpred(intv)}, allEdges.filter{ case (ids, (intv, attr)) => vtpred(intv) && etpred(intv)}, defaultValue, storageLevel)

  }

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
  protected val defvp = (vid: VertexId, attrs: (Interval, VD)) => true
  def select(epred: ((VertexId, VertexId), (Interval, ED)) => Boolean = ((ids, ed) => true), vpred: (VertexId, (Interval, VD)) => Boolean = defvp): TGraphNoSchema[VD, ED] = {
    //if the vpred is not provided, i.e. is true
    //then we can skip most of the work on enforcing integrity constraints with V
    //simple select on vertices, then join the coalesced by structure result
    //to modify edges

    val newVerts: RDD[(VertexId, (Interval, VD))] = if (vpred == defvp) allVertices else allVertices.filter{ case (vid, attrs) => vpred(vid, attrs)}
    val filteredEdges: RDD[((VertexId, VertexId), (Interval, ED))] = allEdges.filter{ case (ids, attrs) => epred(ids, attrs)}

    val newEdges = if (filteredEdges.isEmpty || vpred == defvp) filteredEdges else TGraphNoSchema.constrainEdges(newVerts, filteredEdges)

    //no need to coalesce either vertices or edges because we are removing some entities, but not extending them or modifying attributes

    fromRDDs(newVerts, newEdges, defaultValue, storageLevel)

  }

  protected val vgb = (vid: VertexId, attr: Any) => vid
  override def aggregate(res: WindowSpecification, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED)(vgroupby: (VertexId, VD) => VertexId = vgb): TGraphNoSchema[VD, ED] = {
    if (allVertices.isEmpty)
      return emptyGraph[VD,ED](defaultValue)

    res match {
      case c : ChangeSpec => aggregateByChange(c, vgroupby, vquant, equant, vAggFunc, eAggFunc)
      case t : TimeSpec => aggregateByTime(t, vgroupby, vquant, equant, vAggFunc, eAggFunc)
      case _ => throw new IllegalArgumentException("unsupported window specification")
    }
  }

  protected def aggregateByChange(c: ChangeSpec, vgroupby: (VertexId, VD) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraphNoSchema[VD, ED] = {
    val size: Integer = c.num

    //each tuple interval must be split based on the overall intervals
    val locali = ProgramContext.sc.broadcast(intervals.grouped(size).toList)
    val split: (Interval => List[(Interval, List[Interval])]) = (interval: Interval) => {
      locali.value.flatMap{ group =>
        val res = group.flatMap{ case intv =>
          if (intv.intersects(interval)) Some(intv) else None
        }.toList
        if (res.isEmpty)
          None
        else
          Some(Interval(group.head.start, group.last.end), res)
      }
    }
    val splitVerts: RDD[((VertexId, Interval), (VD, List[Interval]))] = if (vgroupby == vgb) {
      allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii._1), (attr, ii._2)))}
    } else {
      allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vgroupby(vid,attr), ii._1), (attr, ii._2)))}
    }

    val splitEdges: RDD[((VertexId, VertexId, Interval),(ED, List[Interval]))] = if (vgroupby == vgb) {
      allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii._1), (attr, ii._2)))}
    } else {
      val newVIds: RDD[(VertexId, (Interval, VertexId))] = allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, vgroupby(vid, attr)))}

      //for each edge, similar except computing the new ids requires joins with V
      val edgesWithIds: RDD[((VertexId, VertexId), (Interval, ED))] = allEdges.map(e => (e._1._1, e)).join(newVIds).filter{ case (vid, (e, v)) => e._2._1.intersects(v._1)}.map{ case (vid, (e, v)) => (e._1._2, (v._2, (Interval(TempGraphOps.maxDate(e._2._1.start, v._1.start), TempGraphOps.minDate(e._2._1.end, v._1.end)), e._2._2)))}.join(newVIds).filter{ case (vid, (e, v)) => e._2._1.intersects(v._1)}.map{ case (vid, (e, v)) => ((e._1, v._2), (Interval(TempGraphOps.maxDate(e._2._1.start, v._1.start), TempGraphOps.minDate(e._2._1.end, v._1.end)), e._2._2))}
      edgesWithIds.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii._1), (attr, ii._2)))}
    }

    //reduce vertices by key, also computing the total period occupied
    //filter out those that do not meet quantification criteria
    //map to final result
    implicit val ord = TempGraphOps.dateOrdering
    val combine = (lst: List[Interval]) => lst.sortBy(x => x.start).foldLeft(List[Interval]()){ (r,c) => r match {
      case head :: tail =>
        if (head.intersects(c)) Interval(head.start, TempGraphOps.maxDate(head.end, c.end)) :: tail else c :: head :: tail
      case Nil => List(c)
    }}

    val newVerts: RDD[(VertexId, (Interval, VD))] = TGraphNoSchema.coalesce(splitVerts.reduceByKey((a,b) => (vAggFunc(a._1, b._1), a._2 ++ b._2)).filter(v => vquant.keep(combine(v._2._2).map(ii => ii.ratio(v._1._2)).reduce(_ + _))).map(v => (v._1._1, (v._1._2, v._2._1))))
    //same for edges
    val aggEdges: RDD[((VertexId, VertexId), (Interval, ED))] = splitEdges.reduceByKey((a,b) => (eAggFunc(a._1, b._1), a._2 ++ b._2)).filter(e => equant.keep(combine(e._2._2).map(ii => ii.ratio(e._1._3)).reduce(_ + _))).map(e => ((e._1._1, e._1._2), (e._1._3, e._2._1)))

    val newEdges = if (aggEdges.isEmpty) aggEdges else TGraphNoSchema.constrainEdges(newVerts, aggEdges)

    fromRDDs(newVerts, TGraphNoSchema.coalesce(newEdges), defaultValue, storageLevel)

  }

  protected def aggregateByTime(c: TimeSpec, vgroupby: (VertexId, VD) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraphNoSchema[VD, ED] = {
    val start = span.start

    //if there is no structural aggregation, i.e. vgroupby is vid => vid
    //then we can skip the expensive joins
    val splitVerts: RDD[((VertexId, Interval), (VD, List[Interval]))] = if (vgroupby == vgb) {
      //for each vertex, we split it into however many intervals it falls into
      allVertices.flatMap{ case (vid, (intv, attr)) => intv.split(c.res, start).map(ii => ((vid, ii._3), (attr, List(ii._1))))}
    } else {
      allVertices.flatMap{ case (vid, (intv, attr)) => intv.split(c.res, start).map(ii => ((vgroupby(vid,attr), ii._3), (attr, List(ii._1))))}
    }

    val splitEdges: RDD[((VertexId, VertexId, Interval),(ED, List[Interval]))] = if (vgroupby == vgb) {
      allEdges.flatMap{ case (ids, (intv, attr)) => intv.split(c.res, start).map(ii => ((ids._1, ids._2, ii._3), (attr, List(ii._1))))}
    } else {
      val newVIds: RDD[(VertexId, (Interval, VertexId))] = allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, vgroupby(vid, attr)))}

      //for each edge, similar except computing the new ids requires joins with V
      val edgesWithIds: RDD[((VertexId, VertexId), (Interval, ED))] = allEdges.map(e => (e._1._1, e)).join(newVIds).filter{ case (vid, (e, v)) => e._2._1.intersects(v._1)}.map{ case (vid, (e, v)) => (e._1._2, (v._2, (Interval(TempGraphOps.maxDate(e._2._1.start, v._1.start), TempGraphOps.minDate(e._2._1.end, v._1.end)), e._2._2)))}.join(newVIds).filter{ case (vid, (e, v)) => e._2._1.intersects(v._1)}.map{ case (vid, (e, v)) => ((e._1, v._2), (Interval(TempGraphOps.maxDate(e._2._1.start, v._1.start), TempGraphOps.minDate(e._2._1.end, v._1.end)), e._2._2))}
      edgesWithIds.flatMap{ case (ids, (intv, attr)) => intv.split(c.res, start).map(ii => ((ids._1, ids._2, ii._3), (attr, List(ii._1))))}
    }

    //reduce vertices by key, also computing the total period occupied
    //filter out those that do not meet quantification criteria
    //map to final result
    implicit val ord = TempGraphOps.dateOrdering
    val combine = (lst: List[Interval]) => lst.sortBy(x => x.start).foldLeft(List[Interval]()){ (r,c) => r match {
      case head :: tail =>
        if (head.intersects(c)) Interval(head.start, TempGraphOps.maxDate(head.end, c.end)) :: tail else c :: head :: tail
      case Nil => List(c)
    }}
    val newVerts: RDD[(VertexId, (Interval, VD))] = TGraphNoSchema.coalesce(splitVerts.reduceByKey((a,b) => (vAggFunc(a._1, b._1), a._2 ++ b._2)).filter(v => vquant.keep(combine(v._2._2).map(ii => ii.ratio(v._1._2)).reduce(_ + _))).map(v => (v._1._1, (v._1._2, v._2._1))))
    //same for edges
    val aggEdges: RDD[((VertexId, VertexId), (Interval, ED))] = splitEdges.reduceByKey((a,b) => (eAggFunc(a._1, b._1), a._2 ++ b._2)).filter(e => equant.keep(combine(e._2._2).map(ii => ii.ratio(e._1._3)).reduce(_ + _))).map(e => ((e._1._1, e._1._2), (e._1._3, e._2._1)))

    val newEdges = if (aggEdges.isEmpty) aggEdges else TGraphNoSchema.constrainEdges(newVerts, aggEdges)

    fromRDDs(newVerts, TGraphNoSchema.coalesce(newEdges), defaultValue, storageLevel)
  }

  /**
    * Transforms the structural schema of the graph
    * @param emap The mapping function for edges
    * @param vmap The mapping function for vertices
    * @param defaultValue The default value for attribute VD2. Should be something that is not an available value, like Null
    * @return tgraph The transformed graph. The temporal schema is unchanged.
    */
  def project[ED2: ClassTag, VD2: ClassTag](emap: (Edge[ED], Interval) => ED2, vmap: (VertexId, Interval, VD) => VD2, defVal: VD2): TGraphNoSchema[VD2, ED2] = {
    //project may cause coalesce but it does not affect the integrity constraint on E
    //so we don't need to check it
    fromRDDs(TGraphNoSchema.coalesce(allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, vmap(vid, intv, attr)))}), TGraphNoSchema.coalesce(allEdges.map{ case (ids, (intv, attr)) => (ids, (intv, emap(Edge(ids._1, ids._2, attr), intv)))}), defVal, storageLevel)
  }

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
  def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2, defVal: VD2)(implicit eq: VD =:= VD2 = null): TGraphNoSchema[VD2, ED] = {
    fromRDDs(TGraphNoSchema.coalesce(allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, map(vid, intv, attr)))}), allEdges, defaultValue, storageLevel)
  }

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
  def mapEdges[ED2: ClassTag](map: (Edge[ED], Interval) => ED2): TGraphNoSchema[VD, ED2] = {
    fromRDDs(allVertices, TGraphNoSchema.coalesce(allEdges.map{ case (ids, (intv, attr)) => (ids, (intv, map(Edge(ids._1, ids._2, attr), intv)))}), defaultValue, storageLevel)
  }

  override def union(other: TGraph[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TGraphNoSchema[VD, ED] = {
    var grp2: TGraphNoSchema[VD, ED] = other match {
      case grph: TGraphNoSchema[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (span.intersects(grp2.span)) {
      //compute new intervals
      val newIntvs: Seq[Interval] = TempGraphOps.intervalUnion(intervals, grp2.intervals)

      val split = (interval: Interval) => {
        newIntvs.flatMap{ intv =>
          if (intv.intersects(interval))
            Some(intv)
          else
            None
        }
      }

      val newVerts = allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}.union(grp2.allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}).reduceByKey((a,b) => vFunc(a,b)).map{ case (v, attr) => (v._1, (v._2, attr))}
      val newEdges = allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}.union(grp2.allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}).reduceByKey((a,b) => eFunc(a,b)).map{ case (e, attr) => ((e._1, e._2), (e._3, attr))}
      fromRDDs(TGraphNoSchema.coalesce(newVerts), TGraphNoSchema.coalesce(newEdges), defaultValue, storageLevel)


    } else if (span.end == grp2.span.start || span.start == grp2.span.end) {
      //if the two spans are one right after another but do not intersect
      //then we need to coalesce
      fromRDDs(TGraphNoSchema.coalesce(allVertices.union(grp2.vertices)), TGraphNoSchema.coalesce(allEdges.union(grp2.edges)), defaultValue, storageLevel)
    } else {
      //if there is no temporal intersection, then we can just add them together
      //no need to worry about coalesce or constraint on E; all still holds
      fromRDDs(allVertices.union(grp2.vertices), allEdges.union(grp2.edges), defaultValue, storageLevel)
    }
  }

  override def intersection(other: TGraph[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TGraphNoSchema[VD, ED] = {
    var grp2: TGraphNoSchema[VD, ED] = other match {
      case grph: TGraphNoSchema[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (span.intersects(grp2.span)) {
      //compute new intervals
      val newIntvs: Seq[Interval] = TempGraphOps.intervalIntersect(intervals, grp2.intervals)

      val split = (interval: Interval) => {
        newIntvs.flatMap{ intv =>
          if (intv.intersects(interval))
            Some(intv)
          else
            None
        }
      }

      //split the intervals
      //then perform inner join
      val newVertices = allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}.join(grp2.allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}).map{ case ((vid, intv), (attr1, attr2)) => (vid, (intv, vFunc(attr1, attr2)))}

      val newEdges = allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}.join(grp2.allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}).map{ case ((id1, id2, intv), (attr1, attr2)) => ((id1, id2), (intv, eFunc(attr1, attr2)))}

      fromRDDs(TGraphNoSchema.coalesce(newVertices), TGraphNoSchema.coalesce(newEdges), defaultValue, storageLevel)

    } else {
      emptyGraph(defaultValue)
    }

  }

  /**
    * Run pagerank on all intervals. It is up to the implementation to run sequantially,
    * in parallel, incrementally, etc. The number of iterations will depend on both
    * the numIter argument and the rate of convergence, whichever occurs first.
    * @param uni Treat the graph as undirected or directed. true = undirected
    * @param tol epsilon, measure of convergence
    * @param resetProb probability of reset/jump
    * @param numIter number of iterations of the algorithm to run. If omitted, will run
    * until convergence of the tol argument.
    * @return New graph with pageranks for each interval (coalesced)
    */
  def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): TGraphNoSchema[Double, Double]

  /**
   * Run connected components algorithm on a temporal graph
   * return a graph with the vertex value containing the lowest vertex
   * id in the connected component containing that vertex.
   *
   * @return New graph with vertex attribute the id of 
   * the smallest vertex in each connected component 
   * for Intervals in which the vertex appears
   */
  def connectedComponents(): TGraphNoSchema[VertexId, ED]
  
  /**
   * Computes shortest paths to the given set of landmark vertices.
   * @param landmarks the list of landmark vertex ids to which shortest paths will be computed 
   *
   * @return Graph with vertices where each vertex attribute 
   * is the shortest-path distance to each reachable landmark vertex.
   */
  def shortestPaths(landmarks: Seq[VertexId]): TGraphNoSchema[Map[VertexId, Int], ED]

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
  protected def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY): TGraphNoSchema[V, E]

  protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): TGraphNoSchema[V, E]

}

object TGraphNoSchema {
  def computeIntervals[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))]): Seq[Interval] = {
    val dates: RDD[LocalDate] = verts.flatMap{ case (id, (intv, attr)) => List(intv.start, intv.end)}.union(edgs.flatMap { case (ids, (intv, attr)) => List(intv.start, intv.end)}).distinct
    implicit val ord = TempGraphOps.dateOrdering
    dates.sortBy(c => c, true).sliding(2).map(lst => Interval(lst(0), lst(1))).collect()
  }

  /*
   * given an RDD where for a key there is a value over time period
   * coalesce consecutive periods with the same value
   * i.e. (1, (1-3, "blah")) and (1, (3-4, "blah")) become
   * single tuple (1, (1-4, "blah"))
   * Warning: This is a very expensive operation, use sparingly
   */
  def coalesce[K: ClassTag, V: ClassTag](rdd: RDD[(K, (Interval, V))]): RDD[(K, (Interval, V))] = {
    implicit val ord = TempGraphOps.dateOrdering
    //it's faster if we hashpartition first
    rdd.partitionBy(new HashPartitioner(rdd.partitions.size)).persist
      .groupByKey.mapValues{ seq =>  //groupbykey produces RDD[(K, Seq[(p, V)])]
      seq.toSeq.sortBy(x => x._1.start)
        .foldLeft(List[(Interval, V)]()){ (r,c) => r match {
          case head :: tail =>
            if (head._2 == c._2 && head._1.end == c._1.start) (Interval(head._1.start, c._1.end), head._2) :: tail
            else c :: head :: tail
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
    rdd.groupByKey.mapValues{ seq =>  //groupbykey produces RDD[(K, Seq[(p, V)])]
      seq.toSeq.sortBy(x => x._1.start)
        .foldLeft(List[(Interval, V)]()){ (r,c) => r match {
          case head :: tail => if (head._1.end == c._1.start) (Interval(head._1.start, c._1.end), head._2) :: tail else c :: r
          case Nil => List(c)
        }}
    }.flatMap{ case (k,v) => v.map(x => (k, x._1))}
  }

  /*
   * Given an RDD of vertices and an RDD of edges,
   * remove the edges for which there is no src or dst vertex
   * at the indicated time period,
   * shorten the periods as needed to meet the integrity constraint.
   * Warning: This is a very expensive operation, use only when needed.
   */
  def constrainEdges[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))]): RDD[((VertexId, VertexId), (Interval, E))] = {
    val coalescV: RDD[(VertexId, Interval)] = TGraphNoSchema.coalesceStructure(verts)

    //get edges that are valid for each of their two vertices
    val e1: RDD[((VertexId, VertexId), (Interval, E))] = edgs.map(e => (e._1._1, e))
      .join(coalescV) //this creates RDD[(VertexId, (((VertexId, VertexId), (Interval, ED)), Interval))]
      .filter{case (vid, (e, v)) => e._2._1.intersects(v) } //this keeps only matches of vertices and edges where periods overlap
      .map{case (vid, (e, v)) => (e._1, (Interval(TempGraphOps.maxDate(e._2._1.start, v.start), TempGraphOps.minDate(e._2._1.end, v.end)), e._2._2))} //because the periods overlap we don't have to worry that maxdate is after mindate
    val e2: RDD[((VertexId, VertexId), (Interval, E))] = edgs.map(e => (e._1._2, e))
      .join(coalescV) //this creates RDD[(VertexId, (((VertexId, VertexId), (Interval, ED)), Interval))]
      .filter{case (vid, (e, v)) => e._2._1.intersects(v) } //this keeps only matches of vertices and edges where periods overlap
      .map{case (vid, (e, v)) => (e._1, (Interval(TempGraphOps.maxDate(e._2._1.start, v.start), TempGraphOps.minDate(e._2._1.end, v.end)), e._2._2))} //because the periods overlap we don't have to worry that maxdate is after mindate

    //now join them to keep only those that satisfy both foreign key constraints
    e1.join(e2)
    //keep only an edge that meets constraints on both vertex ids
      .filter{ case (k, (e1, e2)) => e1._1.intersects(e2._1) && e1._2 == e2._2 }
      .map{ case (k, (e1, e2)) => (k, (Interval(TempGraphOps.maxDate(e1._1.start, e2._1.start), TempGraphOps.minDate(e1._1.end, e2._1.end)), e1._2))}
  }

}
