package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.reflect.ClassTag
import java.time.LocalDate

import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.graphx._

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps

class VEAGraph(vs: RDD[(VertexId, Interval)], es: RDD[((VertexId, VertexId), Interval)], vas: RDD[(VertexId, (Interval, VertexEdgeAttribute))], eas: RDD[((VertexId, VertexId), (Interval, VertexEdgeAttribute))], schema: GraphSpec = PartialGraphSpec(), storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraphWProperties(schema, storLevel, coal) {

  //TODO: encapsulate each of these into a special type that knows whether
  //the relation is coalesced or not, so avoid unnecessary coalescing
  val allVertices: RDD[(VertexId, Interval)] = vs
  val allEdges: RDD[((VertexId, VertexId), Interval)] = es
  val allVProperties: RDD[(VertexId, (Interval, VertexEdgeAttribute))] = vas
  val allEProperties: RDD[((VertexId, VertexId), (Interval, VertexEdgeAttribute))] = eas

  lazy val intervals: RDD[Interval] = TGraphWProperties.computeIntervals(allVertices, allEdges, allVProperties, allEProperties)
  protected lazy val collectedIntervals: Array[Interval] = intervals.collect

  lazy val span: Interval = computeSpan

  /**
    * The duration the temporal sequence
    */
  override def size(): Interval = span

  override def materialize() = {
    allVertices.count
    allEdges.count
    allVProperties.count
    allEProperties.count
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
  override def vertices: RDD[(VertexId,(Interval, VertexEdgeAttribute))] = coalescedVertices
  override def edges: RDD[((VertexId,VertexId),(Interval, VertexEdgeAttribute))] = coalescedEdges

  /**
    * Get the temporal sequence for the representative graphs
    * composing this tgraph. Intervals are consecutive but
    * not equally sized.
    */
  override def getTemporalSequence: RDD[Interval] = coalescedIntervals

  //because coalesce takes a long time and TGraphs are invariant, want to only
  //do this once
  private lazy val coalescedVertices = {
    if (coalesced)
      allVProperties
    else
      TGraphNoSchema.coalesce(allVProperties)
  }

  //because coalesce takes a long time and TGraphs are invariant, want to only
  //do this once
  private lazy val coalescedEdges = {
    if (coalesced)
      allEProperties
    else
      TGraphNoSchema.coalesce(allEProperties)
  }

  private lazy val coalescedIntervals = {
    if (coalesced)
      intervals
    else
      TGraphWProperties.computeIntervals(TGraphWProperties.coalesce(allVertices), TGraphWProperties.coalesce(allEdges), vertices, edges)
  }

  override def getSnapshot(time: LocalDate): Graph[VertexEdgeAttribute,VertexEdgeAttribute] = {
    if (span.contains(time)) {
      val filteredvas: RDD[(VertexId, VertexEdgeAttribute)] = allVProperties.filter{ case (k,v) => v._1.contains(time)}.map{ case (k,v) => (k, v._2)}
      val filteredeas: RDD[((VertexId, VertexId), VertexEdgeAttribute)] = allEProperties.filter{ case (k,v) => v._1.contains(time)}.map{ case (k,v) => (k, v._2)}
      val filteredvs: RDD[(VertexId, Interval)] = allVertices.filter{ case (k, v) => v.contains(time)}
      val filteredes: RDD[((VertexId, VertexId), Interval)] = allEdges.filter{ case (k,v) => v.contains(time)}

      //because it is possible to have a vertex/edge with no properties for some time t, we need to do an outer join
      val emptyat = VertexEdgeAttribute.empty
      val v = filteredvs.leftOuterJoin(filteredvas).map{ case (k,v) => (k, v._2.getOrElse(emptyat))}
      val e = filteredes.leftOuterJoin(filteredeas).map{ case (k, v) => Edge(k._1, k._2, v._2.getOrElse(emptyat))}

      Graph[VertexEdgeAttribute,VertexEdgeAttribute](v, e, emptyat, storageLevel, storageLevel)
    } else
      Graph[VertexEdgeAttribute,VertexEdgeAttribute](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
  }

  override def coalesce(): VEAGraph = {
    //coalesce the vertices and edges
    //then recompute the intervals and graphs
    if (coalesced)
      this
    else
      fromRDDs(TGraphWProperties.coalesce(allVertices), TGraphWProperties.coalesce(allEdges), TGraphNoSchema.coalesce(allVProperties), TGraphNoSchema.coalesce(allEProperties), graphSpec, storageLevel, true)
  }

  override def slice(bound: Interval): VEAGraph = {
    //VZM: FIXME: this special case is commented out for experimental purposes
    //if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this
    if (!span.intersects(bound)) {
      return emptyGraph
    }

    val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
    val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end
    val selectBound:Interval = Interval(startBound, endBound)

    //slice is correct on coalesced and uncoalesced data
    //and maintains the coalesced/uncoalesced state
    val redFactor = span.ratio(selectBound)
    fromRDDs(allVertices.filter{ case (vid, intv) => intv.intersects(selectBound)}
      .mapValues(y => y.intersection(selectBound).get),
      allEdges.filter{ case (vids, intv) => intv.intersects(selectBound)}
        .mapValues(y => y.intersection(selectBound).get),
      allVProperties.filter{ case (vid, (intv, attr)) => intv.intersects(selectBound)}
        .mapValues(y => (Interval(TempGraphOps.maxDate(y._1.start, startBound), TempGraphOps.minDate(y._1.end, endBound)), y._2)),
      allEProperties.filter{ case (vids, (intv, attr)) => intv.intersects(selectBound)}
        .mapValues(y => (Interval(TempGraphOps.maxDate(y._1.start, startBound), TempGraphOps.minDate(y._1.end, endBound)), y._2)),
      graphSpec, storageLevel, coalesced)
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
  protected val defvp = (vid: VertexId, attrs: (Interval, VertexEdgeAttribute)) => true
  protected val defep = (ids: (VertexId, VertexId), attrs: (Interval, VertexEdgeAttribute)) => true
  def select(epred: ((VertexId, VertexId), (Interval, VertexEdgeAttribute)) => Boolean = defep, vpred: (VertexId, (Interval, VertexEdgeAttribute)) => Boolean = defvp): VEAGraph = {
    //if the vpred is not provided, i.e. is true
    //then we can skip most of the work on enforcing integrity constraints with V
    //simple select on vertices, then join the coalesced by structure result
    //to modify edges

    //select is only correct on coalesced data, thus use vertices/edges methods
    //and thus the result is coalesced (select itself does not cause uncoalesce)
    val newVProps: RDD[(VertexId, (Interval, VertexEdgeAttribute))] = if (vpred == defvp) allVProperties else allVProperties.filter{ case (vid, attrs) => vpred(vid, attrs)}
    //have to constraint vertices. we can just throw away the old ones. only those that survived selection on properties stay anyway, so use those
    val newVerts: RDD[(VertexId, Interval)] = if (vpred == defvp) allVertices else TGraphWProperties.coalesce(newVProps.map{ case (k,v) => (k, v._1)})

    val filtered = if (epred == defep) allEProperties else allEProperties.filter{ case (ids, attrs) => epred(ids, attrs)}
    val newEProps = if (vpred == defvp) filtered else TGraphWProperties.constrainEProperties(newVerts, filtered)
    val newEdges = TGraphWProperties.coalesce(newEProps.map{ case (k,v) => (k, v._1)})

    //no need to coalesce either vertices or edges because we are removing some entities, but not extending them or modifying attributes

    fromRDDs(newVerts, newEdges, newVProps, newEProps, graphSpec, storageLevel, true)

  }

  override def subgraph(epred: ((VertexId, VertexId), VertexEdgeAttribute) => Boolean = defep2, vpred: (VertexId, VertexEdgeAttribute) => Boolean = defvp2): VEAGraph = {
    val newVProps: RDD[(VertexId, (Interval, VertexEdgeAttribute))] = if (vpred == defvp2) allVProperties else allVProperties.filter{ case (vid, attrs) => vpred(vid, attrs._2)}
    val newVerts: RDD[(VertexId, Interval)] = if (vpred == defvp2) allVertices else if (coalesced) TGraphWProperties.coalesce(newVProps.map{ case (k,v) => (k, v._1)}) else newVProps.map{ case (k,v) => (k, v._1)}
    val filtered = if (epred == defep2) allEProperties else allEProperties.filter{ case (ids, attrs) => epred(ids, attrs._2)}
    val newEProps = if (vpred == defvp2) filtered else TGraphWProperties.constrainEProperties(newVerts, filtered)
    val newEdges = if (coalesced) TGraphWProperties.coalesce(newEProps.map{ case (k,v) => (k, v._1)}) else newEProps.map{ case (k,v) => (k, v._1)}

    fromRDDs(newVerts, newEdges, newVProps, newEProps, graphSpec, storageLevel, coalesced)
  }

  override protected def aggregateByChange(c: ChangeSpec, vgroupby: (VertexId, VertexEdgeAttribute) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VertexEdgeAttribute, VertexEdgeAttribute) => VertexEdgeAttribute, eAggFunc: (VertexEdgeAttribute, VertexEdgeAttribute) => VertexEdgeAttribute): VEAGraph = {
    val size: Integer = c.num
    val emptyat = VertexEdgeAttribute.empty

    //each tuple interval must be split based on the overall intervals
    val locali = ProgramContext.sc.broadcast(collectedIntervals.grouped(size).map(ii => Interval(ii.head.start, ii.last.end)).toList)
    val split: (Interval => List[(Interval, Interval)]) = (interval: Interval) => {
      locali.value.flatMap{ intv =>
        val res = intv.intersection(interval)
        if (res.isEmpty)
          None
        else
          Some(intv, res.get)
      }
    }

    if (vgroupby == vgb) {
      //we need to split the vertices to compute quantification
      val splitVerts: RDD[((VertexId, Interval), List[Interval])] = allVertices.flatMap{ case (vid, intv) => split(intv).map(ii => ((vid, ii._1), List(ii._2)))}
      val splitEdges: RDD[((VertexId, VertexId, Interval),List[Interval])] = allEdges.flatMap{ case (ids, intv) => split(intv).map(ii => ((ids._1, ids._2, ii._1), List(ii._2)))}

      //reduce vertices by key, also computing the total period occupied
      //filter out those that do not meet quantification criteria
      //map to final result
      implicit val ord = TempGraphOps.dateOrdering
      val combine = (lst: List[Interval]) => lst.sortBy(x => x.start).foldLeft(List[Interval]()){ (r,c) => r match {
        case head :: tail =>
          if (head.intersects(c)) Interval(head.start, TempGraphOps.maxDate(head.end, c.end)) :: tail else c :: head :: tail
        case Nil => List(c)
      }}

      val newVerts: RDD[(VertexId, Interval)] = splitVerts.reduceByKey((a,b) => a ++ b).filter(v => vquant.keep(combine(v._2).map(ii => ii.ratio(v._1._2)).reduce(_ + _))).map(v => v._1)
      //same for edges
      val aggEdges: RDD[((VertexId, VertexId), Interval)] = splitEdges.reduceByKey((a,b) => a ++ b).filter(e => equant.keep(combine(e._2).map(ii => ii.ratio(e._1._3)).reduce(_ + _))).map(e => ((e._1._1, e._1._2), e._1._3))

      //we only need to enforce the integrity constraint on edges if the vertices have all quantification but edges have exists; otherwise it's maintained naturally
      val newEdges = if (vquant.threshold <= equant.threshold) aggEdges else TGraphWProperties.constrainEdges(newVerts, aggEdges)

      //filter out attributes for vertices/edges that did not meet the quantification
      //TODO: similar to constrainEdges, could use broadcastjoin for small numbers
      //TODO: joins are only necessary if quantification is something other than Exists
      val newVProps: RDD[(VertexId, (Interval, VertexEdgeAttribute))] = allVProperties.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii._1), attr))}.reduceByKey((a,b) => vAggFunc(a, b)).map{ case ((vid, intv), attr) => (vid, (intv, attr))}.join(newVerts).filter{ case (vid, (u, w)) => u._1.intersects(w)}.map{ case (vid, (u, w)) => (vid, u)}
      val newEProps: RDD[((VertexId, VertexId), (Interval, VertexEdgeAttribute))] = allEProperties.flatMap{ case (vids, (intv, attr)) => split(intv).map(ii => ((vids._1, vids._2, ii._1), attr))}.reduceByKey((a,b) => eAggFunc(a,b)).map{ case ((vid1, vid2, intv), attr) => ((vid1, vid2), (intv, attr))}.join(newEdges).filter{ case (vids, (u, w)) => u._1.intersects(w)}.map{ case (vids, (u, w)) => (vids, u)}

      fromRDDs(newVerts, newEdges, newVProps, newEProps, graphSpec, storageLevel, false)

    } else {
      //TODO!
      throw new UnsupportedOperationException("aggregate with structural groupby not supported yet")
    }
  }

  override protected def aggregateByTime(c: TimeSpec, vgroupby: (VertexId, VertexEdgeAttribute) => VertexId, vquant: Quantification, equant: Quantification, vAggFunc: (VertexEdgeAttribute, VertexEdgeAttribute) => VertexEdgeAttribute, eAggFunc: (VertexEdgeAttribute, VertexEdgeAttribute) => VertexEdgeAttribute): VEAGraph = {
    val start = span.start

    //if there is no structural aggregation, i.e. vgroupby is vid => vid
    //then we can skip the expensive joins
    if (vgroupby == vgb) {
      //for each vertex, we split it into however many intervals it falls into
      val splitVerts: RDD[((VertexId, Interval), List[Interval])] = allVertices.flatMap{ case (vid, intv) => intv.split(c.res, start).map(ii => ((vid, ii._2), List(ii._1)))}
      val splitEdges: RDD[((VertexId, VertexId, Interval), List[Interval])] = allEdges.flatMap{ case (ids, intv) => intv.split(c.res, start).map(ii => ((ids._1, ids._2, ii._2), List(ii._1)))}

      //reduce vertices by key, also computing the total period occupied
      //filter out those that do not meet quantification criteria
      //map to final result
      implicit val ord = TempGraphOps.dateOrdering
      //TODO: move this combine function into a util class, since it's used in multiple places
      val combine = (lst: List[Interval]) => lst.sortBy(x => x.start).foldLeft(List[Interval]()){ (r,c) => r match {
        case head :: tail =>
          if (head.intersects(c)) Interval(head.start, TempGraphOps.maxDate(head.end, c.end)) :: tail else c :: head :: tail
        case Nil => List(c)
      }}
      val newVerts: RDD[(VertexId, Interval)] = splitVerts.reduceByKey((a,b) => a ++ b).filter(v => vquant.keep(combine(v._2).map(ii => ii.ratio(v._1._2)).reduce(_ + _))).map(v => (v._1._1, v._1._2))
      //same for edges
      val aggEdges: RDD[((VertexId, VertexId), Interval)] = splitEdges.reduceByKey((a,b) => a ++ b).filter(e => equant.keep(combine(e._2).map(ii => ii.ratio(e._1._3)).reduce(_ + _))).map(e => ((e._1._1, e._1._2), e._1._3))
      val newEdges = if (vquant.threshold <= equant.threshold) aggEdges else TGraphWProperties.constrainEdges(newVerts, aggEdges)

      //filter out attributes for vertices/edges that did not meet the quantification
      //TODO: similar to constrainEdges, could use broadcastjoin for small numbers
      //TODO: joins are only necessary if quantification is something other than Exists
      val newVProps: RDD[(VertexId, (Interval, VertexEdgeAttribute))] = allVProperties.flatMap{ case (vid, (intv, attr)) => intv.split(c.res, start).map(ii => ((vid, ii._1), attr))}.reduceByKey((a,b) => vAggFunc(a, b)).map{ case ((vid, intv), attr) => (vid, (intv, attr))}.join(newVerts).filter{ case (vid, (u, w)) => u._1.intersects(w)}.map{ case (vid, (u, w)) => (vid, u)}
      val newEProps: RDD[((VertexId, VertexId), (Interval, VertexEdgeAttribute))] = allEProperties.flatMap{ case (vids, (intv, attr)) => intv.split(c.res, start).map(ii => ((vids._1, vids._2, ii._1), attr))}.reduceByKey((a,b) => eAggFunc(a,b)).map{ case ((vid1, vid2, intv), attr) => ((vid1, vid2), (intv, attr))}.join(newEdges).filter{ case (vids, (u, w)) => u._1.intersects(w)}.map{ case (vids, (u, w)) => (vids, u)}

      fromRDDs(newVerts, newEdges, newVProps, newEProps, graphSpec, storageLevel, false)

    } else {
      //TODO!
      throw new UnsupportedOperationException("aggregate with structural groupby not supported yet")
    }
  }

  /**
    * Transforms the structural schema of the graph
    * @param emap The mapping function for edges
    * @param vmap The mapping function for vertices
    * @return tgraph The transformed graph. The temporal schema is unchanged.
    */
  override def map(emap: Edge[VertexEdgeAttribute] => VertexEdgeAttribute, vmap: (VertexId, VertexEdgeAttribute) => VertexEdgeAttribute, newSpec: GraphSpec): VEAGraph = {
    //map may cause uncoalesce but it does not affect the integrity constraint on E
    //so we don't need to check it
    //map does not care whether data is coalesced or not
    fromRDDs(allVertices, allEdges, allVProperties.map{ case (vid, (intv, attr)) => (vid, (intv, vmap(vid, attr)))}, allEProperties.map{ case (ids, (intv, attr)) => (ids, (intv, emap(Edge(ids._1, ids._2, attr))))}, newSpec, storageLevel, false)
  }

  /**
    * Transforms each vertex attribute in the graph for each time period
    * using the map function. 
    * Special case of general transform, included here for better compatibility with GraphX.
    *
    * @param map the function from a vertex object to a new vertex value
    */
  override def mapVertices(map: (VertexId, Interval, VertexEdgeAttribute) => VertexEdgeAttribute, newSpec: GraphSpec): VEAGraph = {
    fromRDDs(allVertices, allEdges, allVProperties.map{ case (vid, (intv, attr)) => (vid, (intv, map(vid, intv, attr)))}, allEProperties, newSpec, storageLevel, false)
  }

  /**
   * Transforms each edge attribute in the graph using the map function.  The map function is not
   * passed the vertex value for the vertices adjacent to the edge.  If vertex values are desired,
   * use `mapTriplets`.
   * Special case of general transform, included here for better compatibility with GraphX.
   *
   * @param map the function from an edge object with a time index to a new edge value.
   */
  override def mapEdges(map: (Interval, Edge[VertexEdgeAttribute]) => VertexEdgeAttribute, newSpec: GraphSpec): VEAGraph = {
    fromRDDs(allVertices, allEdges, allVProperties, allEProperties.map{ case (ids, (intv, attr)) => (ids, (intv, map(intv, Edge(ids._1, ids._2, attr))))}, newSpec, storageLevel, false)
  }

  override def union(other: TGraphWProperties): VEAGraph = {
    //union is correct whether the two input graphs are coalesced or not
    var grp2: VEAGraph = other match {
      case grph: VEAGraph => grph
      case _ => throw new ClassCastException //TODO: can transform the other to needed type manually
    }

    if (span.intersects(grp2.span)) {
      //compute new intervals
      val newIntvs: RDD[Interval] = TempGraphOps.intervalUnion(intervals, grp2.intervals)

      //TODO: rewrite to use the newIntvs rdd instead of materializing
      val newIntvsc = ProgramContext.sc.broadcast(newIntvs.collect)
      val split = (interval: Interval) => {
        newIntvsc.value.flatMap{ intv =>
          if (intv.intersects(interval))
            Some(intv)
          else
            None
        }
      }

      val newVerts = allVertices.flatMap{ case (vid, intv) => split(intv).map(ii => ((vid, ii), 1))}.fullOuterJoin(grp2.allVertices.flatMap{ case (vid, intv) => split(intv).map(ii => ((vid, ii), 1))}).map{ case (v, attr) => (v._1, v._2)}
      val newEdges = allEdges.flatMap{ case (ids, intv) => split(intv).map(ii => ((ids._1, ids._2, ii), 1))}.fullOuterJoin(grp2.allEdges.flatMap{ case (ids, intv) => split(intv).map(ii => ((ids._1, ids._2, ii), 1))}).map{ case (e, attr) => ((e._1, e._2), e._3)}
      //now attributes
      val emptyat = VertexEdgeAttribute.empty
      val newVProps = allVProperties.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}.fullOuterJoin(grp2.allVProperties.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}).map{ case (v, attr) => (v._1, (v._2, attr._1.getOrElse(emptyat) ++ attr._2.getOrElse(emptyat)))}
      val newEProps = allEProperties.flatMap{ case (vids, (intv, attr)) => split(intv).map(ii => ((vids._1, vids._2, ii), attr))}.fullOuterJoin(grp2.allEProperties.flatMap{ case (vids, (intv, attr)) => split(intv).map(ii => ((vids._1, vids._2, ii), attr))}).map{ case (e, attr) => ((e._1, e._2), (e._3, attr._1.getOrElse(emptyat) ++ attr._2.getOrElse(emptyat)))}

      fromRDDs(newVerts, newEdges, newVProps, newEProps, graphSpec, storageLevel, false)

    } else {
      //if the two spans are one right after another but do not intersect
      //and the results of coalesced are also coalesced
      //if the two spans touch but do not interest, the results are uncoalesced
      val col = coalesced && grp2.coalesced && span.end != grp2.span.start && span.start != grp2.span.end

      fromRDDs(allVertices.union(grp2.allVertices), allEdges.union(grp2.allEdges), allVProperties.union(grp2.allVProperties), allEProperties.union(grp2.allEProperties),
        graphSpec, storageLevel, col)
    }
  }

  override def intersection(other: TGraphWProperties): VEAGraph = {
    //intersection is correct whether the two input graphs are coalesced or not
    var grp2: VEAGraph = other match {
      case grph: VEAGraph => grph
      case _ => throw new ClassCastException //TODO: can manually convert the other to this type and perform the op anyway
    }

    if (span.intersects(grp2.span)) {
      //compute new intervals
      val newIntvs: RDD[Interval] = TempGraphOps.intervalIntersect(intervals, grp2.intervals)

      //TODO: rewrite to use the newIntvs rdd instead of materializing
      val newIntvsc = ProgramContext.sc.broadcast(newIntvs.collect)
      val split = (interval: Interval) => {
        newIntvsc.value.flatMap{ intv =>
          if (intv.intersects(interval))
            Some(intv)
          else
            None
        }
      }

      //split the intervals
      //then perform inner join
      //TODO: find a more efficient way that avoids splitting except at intersections
      val newVertices = allVertices.flatMap{ case (vid, intv) => split(intv).map(ii => ((vid, ii), 1))}.join(grp2.allVertices.flatMap{ case (vid, intv) => split(intv).map(ii => ((vid, ii), 1))}).map{ case ((vid, intv), attrs) => (vid, intv)}
      val newEdges = allEdges.flatMap{ case (ids, intv) => split(intv).map(ii => ((ids._1, ids._2, ii), 1))}.join(grp2.allEdges.flatMap{ case (ids, intv) => split(intv).map(ii => ((ids._1, ids._2, ii), 1))}).map{ case ((id1, id2, intv), attrs) => ((id1, id2), intv)}
      //now properties. here we need to do outer join and then constrain
      //now attributes
      val emptyat = VertexEdgeAttribute.empty
      val newVProps = allVProperties.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}.fullOuterJoin(grp2.allVProperties.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}).map{ case (v, attr) => (v._1, (v._2, attr._1.getOrElse(emptyat) ++ attr._2.getOrElse(emptyat)))}.join(newVertices).filter{ case (vid, (u, w)) => u._1.intersects(w)}.map{ case (vid, (u, w)) => (vid, (u._1.intersection(w).get, u._2))}
      val newEProps = allEProperties.flatMap{ case (vids, (intv, attr)) => split(intv).map(ii => ((vids._1, vids._2, ii), attr))}.fullOuterJoin(grp2.allEProperties.flatMap{ case (vids, (intv, attr)) => split(intv).map(ii => ((vids._1, vids._2, ii), attr))}).map{ case (e, attr) => ((e._1, e._2), (e._3, attr._1.getOrElse(emptyat) ++ attr._2.getOrElse(emptyat)))}.join(newEdges).filter{ case (vids, (u, w)) => u._1.intersects(w)}.map{ case (vids, (u, w)) => (vids, u)}

      fromRDDs(newVertices, newEdges, newVProps, newEProps, graphSpec, storageLevel, false)

    } else {
      emptyGraph
    }

  }

  /** Analytics */

  override def pregel[A: ClassTag]
  (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
    activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VertexEdgeAttribute, A) => VertexEdgeAttribute,
    sendMsg: EdgeTriplet[VertexEdgeAttribute, VertexEdgeAttribute] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A): VEAGraph = {
    throw new UnsupportedOperationException("analytics not supported")
  }

  override def degree: RDD[(VertexId, (Interval, Int))] = {
    throw new UnsupportedOperationException("degree not supported")
  }

  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): VEAGraph = {
    throw new UnsupportedOperationException("analytics not supported")
  }

  override def connectedComponents(): VEAGraph = {
    throw new UnsupportedOperationException("analytics not supported")
  }

  override def shortestPaths(uni: Boolean, landmarks: Seq[VertexId]): VEAGraph = {
    throw new UnsupportedOperationException("analytics not supported")
  }

  /** Spark-specific */

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): VEAGraph = {
    allVertices.persist(newLevel)
    allEdges.persist(newLevel)
    allVProperties.persist(newLevel)
    allEProperties.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): VEAGraph = {
    allVertices.unpersist(blocking)
    allEdges.unpersist(blocking)
    allVProperties.unpersist(blocking)
    allEProperties.unpersist(blocking)
    this
  }

  override def numPartitions(): Int = 0

  override def partitionBy(tgp: TGraphPartitioning): VEAGraph = {
    return this
  }

  /** Utility methods **/
  protected def computeSpan: Interval = {
    implicit val ord = TempGraphOps.dateOrdering
    val dates = allVertices.flatMap{ case (id, intv) => List(intv.start, intv.end)}.distinct.sortBy(c => c, true, 1)
    if (dates.isEmpty)
      Interval(LocalDate.now, LocalDate.now)
    else
      Interval(dates.min, dates.max)
  }

  protected def fromRDDs(verts: RDD[(VertexId, Interval)], edgs: RDD[((VertexId, VertexId), Interval)], vprops: RDD[(VertexId, (Interval, VertexEdgeAttribute))], eprops: RDD[((VertexId, VertexId), (Interval, VertexEdgeAttribute))], spec: GraphSpec, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false): VEAGraph = {
    VEAGraph.fromRDDs(verts, edgs, vprops, eprops, spec, storLevel, coalesced = coal)
  }

  override protected def emptyGraph: VEAGraph = VEAGraph.emptyGraph
  
}

object VEAGraph extends Serializable {
  def emptyGraph: VEAGraph = new VEAGraph(ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, coal = true)

  def fromRDDs(verts: RDD[(VertexId, Interval)], edgs: RDD[((VertexId, VertexId), Interval)], vprops: RDD[(VertexId, (Interval, VertexEdgeAttribute))], eprops: RDD[((VertexId, VertexId), (Interval, VertexEdgeAttribute))], spec: GraphSpec = PartialGraphSpec(), storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): VEAGraph = {
    val cverts = if (ProgramContext.eagerCoalesce && !coalesced) TGraphWProperties.coalesce(verts) else verts
    val cedges = if (ProgramContext.eagerCoalesce && !coalesced) TGraphWProperties.coalesce(edgs) else edgs
    val cvps = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(vprops) else vprops
    val ceps = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(eprops) else eprops

    val coal = coalesced | ProgramContext.eagerCoalesce

    new VEAGraph(cverts, cedges, cvps, ceps, spec, storLevel, coal)
  }

}
