//this is purely for evaluation purposes for now
//uses parent methods on V&E but is concrete
//cannot compute analytics
package edu.drexel.cs.dbgroup.temporalgraph.representations

import java.time.LocalDate
import java.util.Map

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import scala.reflect.ClassTag

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

    val newVerts: RDD[(VertexId, (Interval, VD))] = allVertices
    val newEdges =  allEdges.map(e => {
          var et=new EdgeTriplet[VD,ED]
          et.srcId = e._1._1
          et.dstId = e._1._2
          et.attr = e._2._2
          (et,e._2._1)
        }
      ).filter(e=> epred(e._1,e._2))
    fromRDDs(newVerts, newEdges.map{ case (e,intv) => ((e.srcId,e.dstId), (intv,e.attr))}, defaultValue, storageLevel, coalesced)


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
    val splitEdges: RDD[((VertexId, VertexId, Interval),(ED, Double))] = allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii._1), (attr, ii._2.ratio(ii._1))))}


    //reduce vertices by key, also computing the total period occupied
    //filter out those that do not meet quantification criteria
    //map to final result
    implicit val ord = TempGraphOps.dateOrdering

    val newVerts: RDD[(VertexId, (Interval, VD))] = splitVerts.reduceByKey((a,b) => (vAggFunc(a._1, b._1), a._2 + b._2)).filter(v => vquant.keep(v._2._2)).map(v => (v._1._1, (v._1._2, v._2._1)))
    //same for edges
    val aggEdges: RDD[((VertexId, VertexId), (Interval, ED))] = splitEdges.reduceByKey((a,b) => (eAggFunc(a._1, b._1), a._2 + b._2)).filter(e => equant.keep(e._2._2)).map(e => ((e._1._1, e._1._2), (e._1._3, e._2._1)))

    //we only need to enforce the integrity constraint on edges if the vertices have all quantification but edges have exists; otherwise it's maintained naturally
    val newEdges = if (vquant.threshold <= equant.threshold) aggEdges else TGraphNoSchema.constrainEdges(newVerts, aggEdges)

    fromRDDs(newVerts, newEdges, defaultValue, storageLevel, false)
  }

  override protected def aggregateByTime(c: TimeSpec, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): VEGraph[VD, ED] = {
    val start = span.start
    //if there is no structural aggregation, i.e. vgroupby is vid => vid
    //then we can skip the expensive joins
    val splitVerts: RDD[((VertexId, Interval), (VD, Double))] = allVertices.flatMap{ case (vid, (intv, attr)) => intv.split(c.res, start).map(ii => ((vid, ii._2), (attr, ii._1.ratio(ii._2))))}

    val splitEdges: RDD[((VertexId, VertexId, Interval),(ED, Double))] = allEdges.flatMap{ case (ids, (intv, attr)) => intv.split(c.res, start).map(ii => ((ids._1, ids._2, ii._2), (attr, ii._1.ratio(ii._2))))}

    //reduce vertices by key, also computing the total period occupied
    //filter out those that do not meet quantification criteria
    //map to final result
    implicit val ord = TempGraphOps.dateOrdering

    val newVerts: RDD[(VertexId, (Interval, VD))] = splitVerts.reduceByKey((a,b) => (vAggFunc(a._1, b._1), a._2 + b._2)).filter(v => vquant.keep(v._2._2)).map(v => (v._1._1, (v._1._2, v._2._1)))
    //same for edges
    val aggEdges: RDD[((VertexId, VertexId), (Interval, ED))] = splitEdges.reduceByKey((a,b) => (eAggFunc(a._1, b._1), a._2 + b._2)).filter(e => equant.keep(e._2._2)).map(e => ((e._1._1, e._1._2), (e._1._3, e._2._1)))
    val newEdges = if (vquant.threshold <= equant.threshold) aggEdges else TGraphNoSchema.constrainEdges(newVerts, aggEdges)
    fromRDDs(newVerts, newEdges, defaultValue, storageLevel, false)
  }


  override def createAttributeNodes(vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED)(vgroupby: (VertexId, VD) => VertexId ): VEGraph[VD, ED] = {

    val splitVerts:  RDD[((VertexId, Interval), VD)]=
      allVertices.map(v=>((vgroupby(v._1,v._2._2),v._2._1),v._2._2))


    val splitEdges: RDD[(((VertexId, VertexId), Interval),ED)] = {
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
      val newEdges = allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}.union(grp2.allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}).reduceByKey((a,b) => eFunc(a,b)).map{ case (e, attr) => ((e._1, e._2), (e._3, attr))}
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
      val newEdges=((allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}).leftOuterJoin((grp2.allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}))) .filter(e=>e._2._2 == None).map{ case (e, attr) => ((e._1, e._2), (e._3, (attr._1)))}
      fromRDDs(newVertices, TGraphNoSchema.constrainEdges(newVertices,newEdges), defaultValue, storageLevel, false)
    } else {
       this
    }
  }

    override def intersection(other: TGraphNoSchema[VD, ED] , vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): VEGraph[VD, ED] = {
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
      val newEdges = allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}.join(grp2.allEdges.flatMap{ case (ids, (intv, attr)) => split(intv).map(ii => ((ids._1, ids._2, ii), attr))}).map{ case ((id1, id2, intv), (attr1, attr2)) => ((id1, id2), (intv, eFunc(attr1, attr2)))}

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
    if (tripletFields == TripletFields.None || tripletFields == TripletFields.EdgeOnly) {
      //for each edge get a message
      var messages: RDD[(VertexId, (Interval, A))] = allEdges.flatMap { e =>
        val et = new EdgeTriplet[VD, ED]
        et.srcId = e._1._1
        et.dstId = e._1._2
        et.attr = e._2._2
        sendMsg(et).map(x => (x._1, List[(Interval, A)]((e._2._1, x._2)))).toSeq
      }.reduceByKey { (a, b) => //group by destination with the merge
        //we have two lists. for each period of intersection, we apply the merge
        var c = List.concat(a, b).sortBy(v => v._1)

        c.foldLeft(List[(Interval, A)]()) { (list, elem) =>
          list match {
            //base case and non-intersection are easy
            case Nil => List(elem)
            case (head :: tail) if !head._1.intersects(elem._1) => {
              elem :: head :: tail
            }
            //intersection
            case (head :: tail) => {
              //handle the empty interval in the case both have the same start date
              val leftSide = Interval.applyOption(head._1.start, elem._1.start) match {
                case None => List[(Interval, A)]()
                case Some(intv) => List[(Interval, A)]((intv, head._2))
              }
              //handle head intersecting elem vs. head containing elem
              val rightSide = head._1.end.compareTo(elem._1.end) match {
                case 0 => List[(Interval, A)]()
                //right side of intersection
                case -1 => List[(Interval, A)]((Interval(head._1.end, elem._1.end), elem._2))
                //right side of contains
                case _ => List[(Interval, A)]((Interval(elem._1.end, head._1.end), head._2))
              }

              rightSide ::: (elem._1, mergeMsg(elem._2, head._2)) :: leftSide ::: tail
            }
          }
        }
      }.flatMap { vl =>
        vl._2.map(x => (vl._1, x))
      }

      //now join with the old values
      var newverts: RDD[(VertexId, (Interval, (VD, A)))] = allVertices.leftOuterJoin(TGraphNoSchema.coalesce(messages))
      //first get things grouped by the original (VertexId,Interval), and a list of edge messages
      //we also have to retain VD for later, so we end up with ((VertexId,Interval), (VD, List[(Interval,A)])
      .map { v => ((v._1, v._2._1._1), (v._2._1._2,List[(Interval, A)](v._2._2.get))) }
      .reduceByKey { (a, b) => (a._1, a._2 ::: b._2) }
      .map{ v =>
        //filtering messages that are outside the vertex interval (since we join on only vid, and we could have vertices with multiple
        //intervals, each (v,interval) could otherwise contain messages from other intervals that don't pertain
        val contained =  v._2._2.filter { i =>
            ((i._1.start.equals(v._1._2.start) || i._1.start.isAfter(v._1._2.start))
              && (i._1.end.equals(v._1._2.end) || i._1.end.isBefore(v._1._2.end)))
        }
        //now append any intervals that got no messages with the default value
        (v._1, (v._2._1, contained ::: Interval.differenceList(v._1._2, contained.map(t => t._1)).map(i => (i, defVal))))
      }
      //produce the result in the proper format
      .flatMap{ v =>
        (v._2._2.map { i =>
          (v._1._1, (i._1, (v._2._1, i._2)))
        })
      }

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

  def fromDataFrames[V: ClassTag, E: ClassTag](verts: org.apache.spark.sql.DataFrame, edgs: org.apache.spark.sql.DataFrame, defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): VEGraph[V, E] = {
    val cverts: RDD[(VertexId, (Interval, V))] = verts.rdd.map(r => (r.getLong(0), (Interval(r.getLong(1), r.getLong(2)), r.getAs[V](3))))
    val ceds: RDD[((VertexId, VertexId), (Interval, E))] = edgs.rdd.map(r => ((r.getLong(0), r.getLong(1)), (Interval(r.getLong(2), r.getLong(3)), r.getAs[E](4))))
    fromRDDs(cverts, ceds, defVal, storLevel, coalesced)
  }

}
