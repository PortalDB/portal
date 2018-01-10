package edu.drexel.cs.dbgroup.portal.representations

import java.time.LocalDate
import java.util.Map

import edu.drexel.cs.dbgroup.portal._
import edu.drexel.cs.dbgroup.portal.util.TempGraphOps
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._


import scala.reflect.ClassTag

class VEGraph[VD: ClassTag, ED: ClassTag](verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[TEdge[ED]], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraphNoSchema[VD,ED](defValue, storLevel, coal) {

  val allVertices: RDD[(VertexId, (Interval, VD))] = verts
  val allEdges: RDD[TEdge[ED]] = edgs
  lazy val intervals: RDD[Interval] = TGraphNoSchema.computeIntervals(allVertices, allEdges)

  lazy val span: Interval = computeSpan

  /**
    * The duration the temporal sequence
    */
  override def size(): Interval = span

  override def materialize() = {
    allVertices.count
    allEdges.count
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
  override def vertices: RDD[(VertexId,(Interval, VD))] = coalescedVertices

  override def edges: RDD[TEdge[ED]] = coalescedEdges

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
      allVertices
    else
      TGraphNoSchema.coalesce(allVertices)
  }

  //because coalesce takes a long time and TGraphs are invariant, want to only
  //do this once
  private lazy val coalescedEdges = {
    if (coalesced)
      allEdges
    else
      TGraphNoSchema.coalesce(allEdges.map(e => e.toPaired())).map(e => TEdge.apply(e._1,e._2))
  }

  private lazy val coalescedIntervals = {
    if (coalesced)
      intervals
    else
      TGraphNoSchema.computeIntervals(vertices, edges)
  }

  override def getSnapshot(time: LocalDate): Graph[VD,(EdgeId,ED)] = {
    if (span.contains(time)) {
      val filteredvas: RDD[(VertexId,VD)] = allVertices.filter{ case (k,v) => v._1.contains(time)}
        .map{ case (k,v) => (k, v._2)}
      val filterededs: RDD[Edge[(EdgeId,ED)]] = allEdges.filter{ e => e.interval.contains(time)}.map{ e => Edge(e.srcId, e.dstId, (e.eId,e.attr))}
      Graph[VD,(EdgeId,ED)](filteredvas, filterededs, defaultValue, storageLevel, storageLevel)
    } else
      Graph[VD,(EdgeId,ED)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
  }

  override def coalesce(): VEGraph[VD, ED] = {
    //coalesce the vertices and edges
    //then recompute the intervals and graphs
    if (coalesced)
      this
    else
      fromRDDs(TGraphNoSchema.coalesce(allVertices), TGraphNoSchema.coalesce(allEdges.map(e => e.toPaired())).map(e => TEdge.apply(e._1,e._2)), defaultValue, storageLevel, true)
  }

  override def slice(bound: Interval): VEGraph[VD, ED] = {
    if (bound.contains(span)) return this
    if (!span.intersects(bound)) {
      return emptyGraph[VD,ED](defaultValue)
    }
    val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
    val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end
    val selectBound:Interval = Interval(startBound, endBound)

    //slice is correct on coalesced and uncoalesced data
    //and maintains the coalesced/uncoalesced state
    fromRDDs(allVertices.filter{ case (vid, (intv, attr)) => intv.intersects(selectBound)}
                  .mapValues(y => (Interval(TempGraphOps.maxDate(y._1.start, startBound), TempGraphOps.minDate(y._1.end, endBound)), y._2)), 
             allEdges.filter{ e => e.interval.intersects(selectBound)}
               .map{te => te.interval = Interval(TempGraphOps.maxDate(te.interval.start, startBound), TempGraphOps.minDate(te.interval.end, endBound))
               te},
      defaultValue, storageLevel, coalesced)
  }

  override def vsubgraph(pred: (VertexId, VD,Interval) => Boolean): VEGraph[VD,ED] = {
    //by calling on vertices, we assure correctness related to coalescing
    val newVerts: RDD[(VertexId, (Interval, VD))] = vertices.filter{ case (vid, attrs) => pred(vid, attrs._2,attrs._1)}
    val newEdges = TGraphNoSchema.constrainEdges(newVerts, allEdges)
    fromRDDs(newVerts, newEdges, defaultValue, storageLevel, coalesced)
  }

  def esubgraphHelper(pred: (TEdgeTriplet[VD,ED]) => Boolean,tripletFields: TripletFields = TripletFields.All ): VEGraph[VD,ED] = {
    //TODO: tripletfields
    if (tripletFields == TripletFields.None || tripletFields == TripletFields.EdgeOnly) {
      val newVerts: RDD[(VertexId, (Interval, VD))] = allVertices
      //by calling on edges instead of allEdges, we assure correctness related to coalescing
      val newEdges = edges.map(e => {
        var et = new TEdgeTriplet[VD,ED]
        et.eId = e.eId
        et.srcId = e.srcId
        et.dstId = e.dstId
        et.attr = e.attr
        et.interval = e.interval
        et
      }
      ).filter(e => pred(e))
      fromRDDs(newVerts, newEdges.map { e => TEdge.apply(e.eId, e.srcId, e.dstId, e.interval, e.attr) }, defaultValue, storageLevel, coalesced)
    }
    else
    {
      val newVerts: RDD[(VertexId, (Interval, VD))] = allVertices
      val newEdges = triplets.filter(e => pred(e))
      fromRDDs(newVerts, newEdges.map { e => TEdge.apply(e.eId, e.srcId, e.dstId, e.interval, e.attr) }, defaultValue, storageLevel, coalesced)
    }
  }

  override def esubgraph(pred: (TEdgeTriplet[VD,ED]) => Boolean,tripletFields: TripletFields = TripletFields.All ): VEGraph[VD,ED] = {
    coalesce().esubgraphHelper(pred, tripletFields)
  }

  override  protected  def aggregateByChange(c: ChangeSpec,  vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): VEGraph[VD, ED] = {
    val size: Integer = c.num

    //each tuple interval must be split based on the overall intervals
    //TODO: get rid of collect if possible
    val locali = ProgramContext.sc.broadcast(intervals.collect.grouped(size).map(ii => Interval(ii.head.start, ii.last.end)).toList)
    val split: (Interval => List[(Interval, Interval)]) = (interval: Interval) => {
      locali.value.flatMap{ intv =>
        val res = intv.intersection(interval)
        if (res.isEmpty)
          None
        else
          Some(intv, res.get)
      }
    }
    val splitVerts: RDD[((VertexId, Interval), (VD, Double))] = allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii._1), (attr, ii._2.ratio(ii._1))))}
    val splitEdges: RDD[((EdgeId,VertexId,VertexId,Interval),(ED,Double))] = allEdges.flatMap{ case e => split(e.interval).map(ii => ((e.eId,e.srcId,e.dstId,ii._1),(e.attr, ii._2.ratio(ii._1))))}


    //reduce vertices by key, also computing the total period occupied
    //filter out those that do not meet quantification criteria
    //map to final result
    implicit val ord = TempGraphOps.dateOrdering

    val newVerts: RDD[(VertexId, (Interval, VD))] = splitVerts.reduceByKey((a,b) => (vAggFunc(a._1, b._1), a._2 + b._2)).filter(v => vquant.keep(v._2._2)).map(v => (v._1._1, (v._1._2, v._2._1)))
    //same for edges
    val aggEdges: RDD[TEdge[ED]] = splitEdges.reduceByKey((a,b) => (eAggFunc(a._1,b._1), a._2 + b._2)).filter(e => equant.keep(e._2._2)).map(e => TEdge[ED](e._1._1,e._1._2,e._1._3,e._1._4,e._2._1))

    //we only need to enforce the integrity constraint on edges if the vertices have all quantification but edges have exists; otherwise it's maintained naturally
    val newEdges = if (vquant.threshold <= equant.threshold) aggEdges else TGraphNoSchema.constrainEdges(newVerts, aggEdges)

    fromRDDs(newVerts, newEdges, defaultValue, storageLevel, false)
  }

  override protected def aggregateByTime(c: TimeSpec, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): VEGraph[VD, ED] = {
    val start = span.start
    //if there is no structural aggregation, i.e. vgroupby is vid => vid
    //then we can skip the expensive joins
    val splitVerts: RDD[((VertexId, Interval), (VD, Double))] = allVertices.flatMap{ case (vid, (intv, attr)) => intv.split(c.res, start).map(ii => ((vid, ii._2), (attr, ii._1.ratio(ii._2))))}

    val splitEdges: RDD[((EdgeId, VertexId, VertexId, Interval),(ED, Double))] = allEdges.flatMap{ e => e.interval.split(c.res, start).map(ii => ((e.eId, e.srcId, e.dstId, ii._2), (e.attr, ii._1.ratio(ii._2))))}

    //reduce vertices by key, also computing the total period occupied
    //filter out those that do not meet quantification criteria
    //map to final result
    implicit val ord = TempGraphOps.dateOrdering

    val newVerts: RDD[(VertexId, (Interval, VD))] = splitVerts.reduceByKey((a,b) => (vAggFunc(a._1, b._1), a._2 + b._2)).filter(v => vquant.keep(v._2._2)).map(v => (v._1._1, (v._1._2, v._2._1)))
    //same for edges
    val aggEdges: RDD[TEdge[ED]] = splitEdges.reduceByKey((a,b) => (eAggFunc(a._1, b._1), a._2 + b._2)).filter(e => equant.keep(e._2._2)).map(e => TEdge[ED](e._1._1,e._1._2,e._1._3,e._1._4,e._2._1))
    val newEdges = if (vquant.threshold <= equant.threshold) aggEdges else TGraphNoSchema.constrainEdges(newVerts, aggEdges)
    fromRDDs(newVerts, newEdges, defaultValue, storageLevel, false)
  }

  override def createAttributeNodes( vAggFunc: (VD, VD) => VD)(vgroupby: (VertexId, VD) => VertexId ): VEGraph[VD, ED] = {

    val locali = ProgramContext.sc.broadcast(intervals.collect)
    //TODO: splitting entities many times causes partitions that are too big
    //need to repartition
    //TODO: it may be better to calculate intervals within each group
    //and split just into those - much less splitting this way
    val split: (Interval => Array[Interval]) = (interval: Interval) => {
      locali.value.flatMap{ intv =>
        if (intv.intersects(interval))
          Some(intv)
        else
          None
      }
    }
    val splitVerts:  RDD[((VertexId, Interval), VD)] = allVertices.flatMap{ 
      case (vid, (intv, attr)) =>
        split(intv).map(ii => ((vgroupby(vid, attr), ii), attr))
      }

    val splitEdges: RDD[TEdge[ED]] = {
      val newVIds: RDD[(VertexId, (Interval, VertexId))] = allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, vgroupby(vid, attr)))}
      //for each edge, similar except computing the new ids requires joins with V
      val edgesWithIds: RDD[TEdge[ED]] = allEdges
        .map{e => (e.srcId, e)}
        .join(newVIds)
        .filter{ case (vid, (e, v)) => e.interval.intersects(v._1)}
        .map{ case (vid, (e, v)) => (e.dstId, (v._2, (Interval(TempGraphOps.maxDate(e.interval.start, v._1.start), TempGraphOps.minDate(e.interval.end, v._1.end)), (e.eId,e.attr))))}
        .join(newVIds)
        .filter{ case (vid, (e, v)) => e._2._1.intersects(v._1)}
        .map{ case (vid, (e, v)) => TEdge(e._2._2._1, e._1, v._2, Interval(TempGraphOps.maxDate(e._2._1.start, v._1.start), TempGraphOps.minDate(e._2._1.end, v._1.end)), e._2._2._2)}
      //FIXME: I think this line is unnecessary
      edgesWithIds.flatMap{ e => split(e.interval).map(ii => TEdge(e.eId,e.srcId, e.dstId, ii, e.attr))}
    }

    //map to final result
    val newVerts: RDD[(VertexId, (Interval, VD))] = splitVerts.reduceByKey((a,b) => (vAggFunc(a, b))).map(v => (v._1._1, (v._1._2, v._2)))

    fromRDDs(newVerts, splitEdges, defaultValue, storageLevel, false)

  }


  /**
    * Transforms each vertex attribute in the graph for each time period
    * using the map function. 
    * Special case of general transform, included here for better compatibility with GraphX.
    *
    * @param map the function from a vertex object to a new vertex value
    * @tparam VD2 the new vertex data type
    *
    */
  override def vmap[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2, defVal: VD2)(implicit eq: VD =:= VD2 = null): VEGraph[VD2, ED] = {
    fromRDDs(allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, map(vid, intv, attr)))}, allEdges, defVal, storageLevel, false)
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
  override def emap[ED2: ClassTag](map: TEdge[ED] => ED2): VEGraph[VD, ED2] = {
    fromRDDs(allVertices, allEdges.map{ e => TEdge[ED2](e.eId,e.srcId,e.dstId, e.interval, map(e))}, defaultValue, storageLevel, false)
  }

  override def union(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): VEGraph[VD, ED] = {
    //union is correct whether the two input graphs are coalesced or not
    var grp2: VEGraph[VD, ED] = other match {
      case grph: VEGraph[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (span.intersects(grp2.span)) {
      //compute new intervals
      val newIntvs: RDD[Interval] = TempGraphOps.intervalUnion(intervals, grp2.intervals)

      //TODO: it is more efficient to split intervals only within their respective groups
      val newIntvsc = ProgramContext.sc.broadcast(newIntvs.collect)
      val split = (interval: Interval) => {
        newIntvsc.value.flatMap{ intv =>
          if (intv.intersects(interval))
            Some(intv)
          else
            None
        }
      }
      val newVerts = allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}.union(grp2.allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}).reduceByKey(vFunc).map{ case (v, attr) => (v._1, (v._2, attr))}
      val newEdges = allEdges.flatMap{ e => split(e.interval).map(ii => ((e.eId,e.srcId, e.dstId, ii), e.attr))}.union(grp2.allEdges.flatMap{ e => split(e.interval).map(ii => ((e.eId,e.srcId,e.dstId, ii), e.attr))}).reduceByKey((a,b) => eFunc(a,b)).map{ e => TEdge.apply(e._1,e._2)}
      fromRDDs(newVerts, newEdges, (defaultValue), storageLevel, false)

    } else {
      //if the two spans are one right after another but do not intersect
      //and the results of coalesced are also coalesced
      //if the two spans touch but do not interest, the results are uncoalesced
      val col = coalesced && grp2.coalesced && span.end != grp2.span.start && span.start != grp2.span.end
      fromRDDs(allVertices.union(grp2.allVertices), allEdges.union(grp2.allEdges),
        (defaultValue), storageLevel, col)
    }
  }

  override def difference(other: TGraphNoSchema[VD, ED]): VEGraph[VD, ED] = {
    val grp2: VEGraph[VD, ED] = other match {
      case grph: VEGraph[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (span.intersects(grp2.span)) {
      //compute new intervals
      val newIntvs: RDD[Interval] = TempGraphOps.intervalDifference(intervals, grp2.intervals)
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
      val newVertices=((allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}).leftOuterJoin((grp2.allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}))).filter(v=>v._2._2 == None).map{ case (v,attr) => (v._1,(v._2,(attr._1)))}
      val newEdges=((allEdges.flatMap{ e => split(e.interval).map(ii => ((e.eId,e.srcId,e.dstId, ii), e.attr))}).leftOuterJoin((grp2.allEdges.flatMap{ e => split(e.interval).map(ii => ((e.eId,e.srcId,e.dstId, ii), e.attr))}))).filter(e=> e._2._2 == None).map{ case (e, attr) => TEdge.apply(e,attr._1)}
      fromRDDs(newVertices, TGraphNoSchema.constrainEdges(newVertices,newEdges), defaultValue, storageLevel, false)
    } else {
      this
    }
  }

  override def intersection(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): VEGraph[VD, ED] = {
    //intersection is correct whether the two input graphs are coalesced or not
    var grp2: VEGraph[VD, ED] = other match {
      case grph: VEGraph[VD, ED] => grph
      case _ => throw new ClassCastException
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
      val newVertices = allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}.join(grp2.allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}).map{ case ((vid, intv), (attr1, attr2)) => (vid, (intv, vFunc(attr1, attr2)))}
      val newEdges = allEdges.flatMap{ e => split(e.interval).map(ii => ((e.eId,e.srcId,e.dstId, ii), e.attr))}.join(grp2.allEdges.flatMap{ e => split(e.interval).map(ii => ((e.eId,e.srcId,e.dstId, ii), e.attr))}).map{ case ((eId,id1, id2, intv), (attr1, attr2)) => TEdge.apply((eId,id1, id2), (intv, eFunc(attr1, attr2)))}

      fromRDDs(newVertices, newEdges, defaultValue, storageLevel, false)

    } else {
      emptyGraph(defaultValue)
    }

  }

  /** Analytics */

  override def pregel[A: ClassTag]
  (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
    activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
    sendMsg: EdgeTriplet[VD, (EdgeId,ED)] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A): VEGraph[VD, ED] = {
    throw new UnsupportedOperationException("analytics not supported")
  }

  override def degree: RDD[(VertexId, (Interval, Int))] = {
    throw new UnsupportedOperationException("degree not supported")
  }

  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): VEGraph[(VD, Double), ED] = {
    throw new UnsupportedOperationException("analytics not supported")
  }

  override def connectedComponents(): VEGraph[(VD, VertexId), ED] = {
    throw new UnsupportedOperationException("analytics not supported")
  }

  override def shortestPaths(uni: Boolean, landmarks: Seq[VertexId]): VEGraph[(VD, Map[VertexId, Int]), ED] = {
    throw new UnsupportedOperationException("analytics not supported")
  }

  override def triangleCount(): VEGraph[(VD, Int), ED] = {
    throw new UnsupportedOperationException("analytics not supported")
  }

  override def clusteringCoefficient: VEGraph[(VD,Double), ED] = {
    throw new UnsupportedOperationException("ccoeff")
  }

  override def aggregateMessages[A: ClassTag](sendMsg: TEdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A, defVal: A, tripletFields: TripletFields = TripletFields.All): VEGraph[(VD, A), ED] = {
    val trips: RDD[TEdgeTriplet[VD,ED]] = if (tripletFields == TripletFields.None || tripletFields == TripletFields.EdgeOnly) {
      allEdges.map { e =>
        val et = new TEdgeTriplet[VD,ED]
        et.eId = e.eId
        et.srcId = e.srcId
        et.dstId = e.dstId
        et.interval = e.interval
        et.attr = e.attr
        et
      }
    } else triplets

    //for each edge get a message
    var messages: RDD[(VertexId, List[(Interval, A)])] = trips.flatMap { et => sendMsg(et).map(x => (x._1, List[(Interval, A)]((et.interval, x._2)))).toSeq}
      .reduceByKey { (a, b) => TempGraphOps.mergeIntervalLists(mergeMsg, a, b) }

    //now join with the old values
    var newverts: RDD[(VertexId, (Interval, (VD, A)))] = allVertices.leftOuterJoin(messages).flatMap { 
      case (vid, (vdata, Some(msg))) => {
        val contained = TempGraphOps.coalesceIntervals(msg).filter(ii => ii._1.intersects(vdata._1))
        if (contained.size > 0) (contained ::: vdata._1.differenceList(contained.map(_._1)).map(ii => (ii, defVal))).map(ii => (vid, (ii._1, (vdata._2, ii._2)))) else List((vid, (vdata._1, (vdata._2, defVal))))
      }
      case (vid, (vdata, None)) => Some((vid, (vdata._1, (vdata._2, defVal))))
    }

    fromRDDs(newverts, allEdges, (defaultValue, defVal), storageLevel, coalesced)
  }

  /** Spark-specific */

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): VEGraph[VD, ED] = {
    allVertices.persist(newLevel)
    allEdges.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): VEGraph[VD, ED] = {
    allVertices.unpersist(blocking)
    allEdges.unpersist(blocking)

    this
  }

  override def numPartitions(): Int = 0

  override def partitionBy(tgp: TGraphPartitioning): VEGraph[VD, ED] = {
    return this
  }

  /** Utility methods **/
  protected def computeSpan: Interval = {
    implicit val ord = TempGraphOps.dateOrdering
    val dates = allVertices.flatMap{ case (id, (intv, attr)) => List(intv.start, intv.end)}.distinct.sortBy(c => c, true, 1)
    if (dates.isEmpty)
      Interval(LocalDate.now, LocalDate.now)
    else
      Interval(dates.min, dates.max)
  }

  override def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[TEdge[E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false): VEGraph[V, E] = {
    VEGraph.fromRDDs(verts, edgs, defVal, storLevel, coalesced = coal)
  }

  override protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): VEGraph[V, E] = VEGraph.emptyGraph(defVal)

  private def triplets: RDD[(TEdgeTriplet[VD, ED])] = {
    allEdges.map(e => (e.srcId, e))
      .join(allVertices) //this creates RDD[(VertexId, (((VertexId, VertexId), (Interval, ED)), Interval))]
      .filter{case (vid, (e, v)) => e.interval.intersects(v._1) } //this keeps only matches of vertices and edges where periods overlap
      .map{case (vid, (e, v)) => (e.dstId, (e, v))}       //((e._1, e._2._1), (e._2._2, v))}
      .join(allVertices) //this creates RDD[(VertexId, ((((VertexId, VertexId), (Interval, ED)), Interval), Interval)
      .filter{ case (vid, (e, v)) => e._1.interval.intersects(v._1) && e._2._1.intersects(v._1)}
      .map {case (vid, (e, v)) =>{
        var et= new TEdgeTriplet[VD,ED]
        et.eId=e._1.eId
        et.srcId=e._1.srcId
        et.dstId=e._1.dstId
        et.interval = Interval(TempGraphOps.maxDate(v._1.start, e._1.interval.start, e._2._1.start), TempGraphOps.minDate(v._1.end, e._1.interval.end, e._2._1.end))
        et.attr=e._1.attr
        et.srcAttr=e._2._2
        et.dstAttr=v._2
        et
      }
    }
  }

}

object VEGraph extends Serializable {
  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): VEGraph[V, E] = new VEGraph(ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, defVal, coal = true)

  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[TEdge[E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): VEGraph[V, E] = {
    val cverts = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(verts) else verts
    val cedges = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(edgs.map(e => e.toPaired())).map(e => TEdge.apply(e._1,e._2)) else edgs
    val coal = coalesced | ProgramContext.eagerCoalesce

    new VEGraph(cverts, cedges, defVal, storLevel, coal)
  }

  def fromDataFrames[V: ClassTag, E: ClassTag](verts: org.apache.spark.sql.DataFrame, edgs: org.apache.spark.sql.DataFrame, defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): VEGraph[V, E] = {
    val cverts: RDD[(VertexId, (Interval, V))] = verts.rdd.map(r => (r.getLong(0), (Interval(r.getLong(1), r.getLong(2)), r.getAs[V](3))))
    val ceds: RDD[TEdge[E]] = edgs.rdd.map(r => TEdge[E](r.getLong(0),r.getLong(1), r.getLong(2), Interval(r.getLong(3), r.getLong(4)), r.getAs[E](5)))
    fromRDDs(cverts, ceds, defVal, storLevel, coalesced)
  }

}
