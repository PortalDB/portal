//this is purely for evaluation purposes for now
//uses parent methods on V&E but is concrete
//cannot compute analytics
package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.reflect.ClassTag
import java.util.Map
import java.time.LocalDate

import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.graphx._

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps

class VEGraph[VD: ClassTag, ED: ClassTag](verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[((VertexId, VertexId), (Interval, ED))], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraphNoSchema[VD,ED](defValue, storLevel, coal) {

  val allVertices: RDD[(VertexId, (Interval, VD))] = verts
  val allEdges: RDD[((VertexId, VertexId), (Interval, ED))] = edgs
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

  override def edges: RDD[((VertexId,VertexId),(Interval, ED))] = coalescedEdges

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
      TGraphNoSchema.coalesce(allEdges)
  }

  private lazy val coalescedIntervals = {
    if (coalesced)
      intervals
    else
      TGraphNoSchema.computeIntervals(vertices, edges)
  }

  override def getSnapshot(time: LocalDate): Graph[VD,ED] = {
    if (span.contains(time)) {
      val filteredvas: RDD[(VertexId,VD)] = allVertices.filter{ case (k,v) => v._1.contains(time)}
        .map{ case (k,v) => (k, v._2)}
      val filterededs: RDD[Edge[ED]] = allEdges.filter{ case (k,v) => v._1.contains(time)}.map{ case (k,v) => Edge(k._1, k._2, v._2)}
      Graph[VD,ED](filteredvas, filterededs, defaultValue, storageLevel, storageLevel)
    } else
      Graph[VD,ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
  }

  override def coalesce(): VEGraph[VD, ED] = {
    //coalesce the vertices and edges
    //then recompute the intervals and graphs
    if (coalesced)
      this
    else
      fromRDDs(TGraphNoSchema.coalesce(allVertices), TGraphNoSchema.coalesce(allEdges), defaultValue, storageLevel, true)
  }

  override def slice(bound: Interval): VEGraph[VD, ED] = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this
    if (!span.intersects(bound)) {
      return emptyGraph[VD,ED](defaultValue)
    }

    val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
    val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end
    val selectBound:Interval = Interval(startBound, endBound)

    //slice is correct on coalesced and uncoalesced data
    //and maintains the coalesced/uncoalesced state
    val redFactor = span.ratio(selectBound)
    fromRDDs(allVertices.filter{ case (vid, (intv, attr)) => intv.intersects(selectBound)}
                  .mapValues(y => (Interval(TempGraphOps.maxDate(y._1.start, startBound), TempGraphOps.minDate(y._1.end, endBound)), y._2)), 
             allEdges.filter{ case (vids, (intv, attr)) => intv.intersects(selectBound)}
                  .mapValues(y => (Interval(TempGraphOps.maxDate(y._1.start, startBound), TempGraphOps.minDate(y._1.end, endBound)), y._2)), 
             defaultValue, storageLevel, coalesced)
  }


  override def vsubgraph(vpred: (VertexId, VD,Interval) => Boolean): VEGraph[VD,ED] = {
    val newVerts: RDD[(VertexId, (Interval, VD))] =allVertices.filter{ case (vid, attrs) => vpred(vid, attrs._2,attrs._1)}
    val newEdges = TGraphNoSchema.constrainEdges(newVerts, allEdges)
    fromRDDs(newVerts, newEdges, defaultValue, storageLevel, coalesced)
  }
  override def esubgraph(epred: (EdgeTriplet[VD,ED],Interval) => Boolean ): VEGraph[VD,ED] = {
    //Todo: implement
    throw  new NotImplementedError()
    // /val newVerts: RDD[(VertexId, (Interval, VD))] = allVertices
    //val newEdges = allEdges.filter{ case (ids,attrs) =>epred((ids,attrs._2),(attrs._1))}
    //fromRDDs(newVerts, newEdges, defaultValue, storageLevel, coalesced)
  }

  override protected def aggregateByChange(c: ChangeSpec,  vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): VEGraph[VD, ED] = {
     //Todo: implement
      throw new NotImplementedError();
  }

  override protected def aggregateByTime(c: TimeSpec, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): VEGraph[VD, ED] = {
  //Todo: implement
    throw new NotImplementedError()
  }


  override def createAttributeNodes(vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED)(vgroupby: (VertexId, VD) => VertexId = vgb): VEGraph[VD, ED]={



    val splitVerts:  RDD[((VertexId, Interval), VD)]= if (vgroupby == vgb) {
      allVertices.map(v=>((v._1,v._2._1),v._2._2))
    } else {
      allVertices.map(v=>((vgroupby(v._1,v._2._2),v._2._1),v._2._2))
    }

    val splitEdges: RDD[(((VertexId, VertexId), Interval),ED)] = if (vgroupby == vgb) {
      allEdges.map(e=>(((e._1._1,e._1._2),e._2._1),e._2._2))
    } else {
      val newVIds: RDD[(VertexId, (Interval, VertexId))] = allVertices.map(v=>(vgroupby(v._1,v._2._2),(v._2._1,v._1)))
      //for each edge, similar except computing the new ids requires joins with V
      val edgesWithIds: RDD[((VertexId, VertexId), (Interval, ED))] = allEdges.map(e => (e._1._1, e)).join(newVIds).filter{ case (vid, (e, v)) => e._2._1.intersects(v._1)}.map{ case (vid, (e, v)) => (e._1._2, (v._2, (Interval(TempGraphOps.maxDate(e._2._1.start, v._1.start), TempGraphOps.minDate(e._2._1.end, v._1.end)), e._2._2)))}.join(newVIds).filter{ case (vid, (e, v)) => e._2._1.intersects(v._1)}.map{ case (vid, (e, v)) => ((e._1, v._2), (Interval(TempGraphOps.maxDate(e._2._1.start, v._1.start), TempGraphOps.minDate(e._2._1.end, v._1.end)), e._2._2))}
      edgesWithIds.map(e=>(((e._1._1,e._1._2),e._2._1),e._2._2))
    }

    //map to final result


    val newVerts: RDD[(VertexId, (Interval, VD))] = splitVerts.reduceByKey((a,b) => (vAggFunc(a, b))).map(v => (v._1._1, (v._1._2, v._2)))
    //same for edges
    val aggEdges: RDD[((VertexId, VertexId), (Interval, ED))] = splitEdges.reduceByKey((a,b) => (eAggFunc(a, b))).map(e=>(((e._1._1._1,e._1._1._1),(e._1._2,e._2))))
    fromRDDs(newVerts, aggEdges, defaultValue, storageLevel, false)


  }

  override def createTemporalNodes(res: WindowSpecification, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): VEGraph[VD, ED]={
    throw  new NotImplementedError()
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
  override def vmap[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2, defVal: VD2)(implicit eq: VD =:= VD2 = null): VEGraph[VD2, ED] = {
    fromRDDs(allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, map(vid, intv, attr)))}, allEdges, defaultValue, storageLevel, false)
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
  override def emap[ED2: ClassTag](map: (Interval, Edge[ED]) => ED2): VEGraph[VD, ED2] = {
    fromRDDs(allVertices, allEdges.map{ case (ids, (intv, attr)) => (ids, (intv, map(intv, Edge(ids._1, ids._2, attr))))}, defaultValue, storageLevel, false)
  }

  override def union(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): VEGraph[Set[VD], Set[ED]] = {
    //union is correct whether the two input graphs are coalesced or not
    var grp2: VEGraph[VD, ED] = other match {
      case grph: VEGraph[VD, ED] => grph
      case _ => throw new ClassCastException
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
      //Todo: Add reducebykey
      val newVerts = allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}.fullOuterJoin(grp2.allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}).map{ case (v, attr) => (v._1, (v._2, (attr._1.toList ++ attr._2.toList).toSet))}
      val newEdges = allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}.fullOuterJoin(grp2.allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}).map{ case (e, attr) => ((e._1, e._2), (e._3, (attr._1.toList ++ attr._2.toList).toSet))}
      fromRDDs(newVerts, newEdges, Set(defaultValue), storageLevel, false)

    } else {
      //if the two spans are one right after another but do not intersect
      //and the results of coalesced are also coalesced
      //if the two spans touch but do not interest, the results are uncoalesced
      val col = coalesced && grp2.coalesced && span.end != grp2.span.start && span.start != grp2.span.end
      val verts1: RDD[(VertexId, (Interval, Set[VD]))] = allVertices.mapValues{ case (intv, attr) => (intv, Set(attr))}
      val verts2: RDD[(VertexId, (Interval, Set[VD]))] = grp2.allVertices.mapValues{ case (intv, attr) => (intv, Set(attr))}
      val edg1: RDD[((VertexId,VertexId),(Interval,Set[ED]))] = allEdges.mapValues{ case (intv, attr) => (intv, Set(attr))}
      val edg2: RDD[((VertexId,VertexId),(Interval,Set[ED]))] = grp2.allEdges.mapValues{ case (intv, attr) => (intv, Set(attr))}
      fromRDDs(verts1.union(verts2), edg1.union(edg2), 
        Set(defaultValue), storageLevel, col)
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
      val newEdges=((allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}).leftOuterJoin((grp2.allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}))) .filter(e=>e._2._2 == None).map{ case (e, attr) => ((e._1, e._2), (e._3, (attr._1)))}
      fromRDDs(newVertices, TGraphNoSchema.constrainEdges(newVertices,newEdges), defaultValue, storageLevel, false)
    } else {
       this
    }
  }

    override def intersection(other: TGraphNoSchema[VD, ED] , vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): VEGraph[Set[VD], Set[ED]] = {
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
      //TODO: find a more efficient way that avoids splitting except at intersections
      //TODO: Add reduceByKey
      val newVertices = allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}.join(grp2.allVertices.flatMap{ case (vid, (intv, attr)) => split(intv).map(ii => ((vid, ii), attr))}).map{ case ((vid, intv), attrs) => (vid, (intv, Set(attrs._1, attrs._2)))}

      val newEdges = allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}.join(grp2.allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}).map{ case ((id1, id2, intv), attrs) => ((id1, id2), (intv, Set(attrs._1, attrs._2)))}

      fromRDDs(newVertices, newEdges, Set(defaultValue), storageLevel, false)

    } else {
      emptyGraph(Set(defaultValue))
    }

  }

  /** Analytics */

  override def pregel[A: ClassTag]
  (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
    activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
    sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
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

  override def aggregateMessages[A: ClassTag](sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A, defVal: A, tripletFields: TripletFields = TripletFields.All): VEGraph[(VD, A), ED] = {
    if (tripletFields == TripletFields.None) {
      //for each edge get a message
      val res: RDD[(VertexId, (Interval, A))] = allEdges.flatMap{e =>
        val et = new EdgeTriplet[VD,ED]
        et.srcId = e._1._1
        et.dstId = e._1._2
        et.attr = e._2._2
        sendMsg(et).map(x => (x._1, List[(Interval,A)]((e._2._1, x._2)))).toSeq
      }.reduceByKey{(a,b) => //group by destination with the merge
        //we have two lists. for each period of intersection, we apply the merge
        val res = for {
          x <- a; y <- b
          if x._1.intersects(y._1)
        } yield x._1.difference(y._1).map((_, x._2)) ++ List[(Interval,A)]((x._1.intersection(y._1).get, mergeMsg(x._2, y._2))) ++ y._1.difference(x._1).map((_, y._2))
          res.flatten
      }.flatMap{vl =>
        vl._2.map(x => (vl._1, x))
      }

      //now join with the old values
      //FIXME: this assumes that there is a new value generated for each possible old interval of an attribute, which is not a given - this potentially throws away values!
      val newverts: RDD[(VertexId, (Interval, (VD, A)))] = allVertices.leftOuterJoin(TGraphNoSchema.coalesce(res)).filter{ case (k, (v,u)) => u.isEmpty || v._1.intersects(u.get._1)}
        .mapValues{ case (v, u) => if (u.isEmpty) (v._1, (v._2, defVal)) else (v._1.intersection(u.get._1).get, (v._2, u.get._2))}

      fromRDDs(newverts, allEdges, (defaultValue, defVal), storageLevel, coalesced)

    } else
      throw new UnsupportedOperationException("aggregateMsg not supported")
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

  protected def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false): VEGraph[V, E] = {
    VEGraph.fromRDDs(verts, edgs, defVal, storLevel, coalesced = coal)
  }

  override protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): VEGraph[V, E] = VEGraph.emptyGraph(defVal)

}

object VEGraph extends Serializable {
  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): VEGraph[V, E] = new VEGraph(ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, defVal, coal = true)

  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[((VertexId, VertexId), (Interval, E))], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): VEGraph[V, E] = {
    val cverts = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(verts) else verts
    val cedges = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(edgs) else edgs
    val coal = coalesced | ProgramContext.eagerCoalesce

    new VEGraph(cverts, cedges, defVal, storLevel, coal)
  }

}
