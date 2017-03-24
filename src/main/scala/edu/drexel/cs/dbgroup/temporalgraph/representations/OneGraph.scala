package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.JavaConversions._
import collection.JavaConverters._
import scala.collection.immutable.BitSet
import scala.collection.breakOut
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import scala.util.control._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd._
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.graphx.impl.GraphXPartitionExtension._

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps

import java.time.LocalDate
import java.util.Map
import java.util.HashSet
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap

/**
  * One graph with attributes stored together
  * Warning: this has a limitation in that there can be only as many
  * tuples of a specific vertex as largest array
  */
class OneGraph[VD: ClassTag, ED: ClassTag](intvs: Array[Interval], grps: Graph[Array[(Interval,VD)], (EdgeId, Array[(Interval,ED)])], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends TGraphNoSchema[VD, ED](defValue, storLevel, coal) {

  protected var partitioning = TGraphPartitioning(PartitionStrategyType.None, 1, 0)
  protected val intervals: Array[Interval] = intvs
  lazy val span: Interval = if (intervals.size > 0) Interval(intervals.head.start, intervals.last.end) else Interval(LocalDate.now, LocalDate.now)
  protected val graphs: Graph[Array[(Interval,VD)], (EdgeId, Array[(Interval,ED)])] = grps

  //FIXME: add eagerCoalesce stuff in each method
  //TODO: find a more efficient way to create triplets, i.e. tie vertex values
  //to edge values for the correct interval throughout this class
  //TODO: adding a bitmap to represent presence/absence of vertex
  //in each interval may make this class more efficient

  /**
    * The duration the temporal sequence
    */
  override def size(): Interval = span

  override def materialize() = {
    graphs.vertices.count
    graphs.edges.count
  }

  override def vertices: RDD[(VertexId, (Interval, VD))] = coalescedVertices

  private lazy val coalescedVertices = {
    val vs: RDD[(VertexId, Array[(Interval,VD)])] = if (coalesced)
      graphs.vertices
    else
      graphs.vertices.mapValues(attr => TempGraphOps.coalesceIntervals(attr.toList).toArray)
    vs.flatMap{ case (vid, attr) => attr.map(xx => (vid, xx))}
  }

  lazy val verticesRaw: RDD[(VertexId, (Interval, VD))] = {
    graphs.vertices.flatMap{ case (vid, attr) => attr.map(xx => (vid, xx))}
  }

  override def edges: RDD[TEdge[ED]] = coalescedEdges

  private lazy val coalescedEdges = {
    val es: EdgeRDD[(EdgeId, Array[(Interval, ED)])] = if (coalesced) graphs.edges else
      graphs.edges.mapValues(e => (e.attr._1, TempGraphOps.coalesceIntervals(e.attr._2.toList).toArray))
    es.flatMap(e => e.attr._2.map(xx =>
      TEdge((e.attr._1, e.srcId, e.dstId), xx)
    ))
  }

  lazy val edgesRaw: RDD[TEdge[ED]] = {
    graphs.edges.flatMap(e => e.attr._2.map(xx =>
      TEdge((e.attr._1, e.srcId, e.dstId), xx)
    ))
  }

  /**
    * Get the temporal sequence for the representative graphs
    * composing this tgraph. Intervals are consecutive but
    * not equally sized.
    */
  override def getTemporalSequence: RDD[Interval] = coalescedIntervals

  private lazy val coalescedIntervals = {
    if (coalesced)
      ProgramContext.sc.parallelize(intervals)
    else
      TGraphNoSchema.computeIntervals(vertices, edges)    
  }

  /** Query operations */
  override def getSnapshot(time: LocalDate): Graph[VD,(EdgeId,ED)] = {
    if (span.contains(time)) {
      graphs.subgraph(vpred = (vid, attr) => attr.filter(x => x._1.contains(time)).size > 0, epred = e => e.attr._2.filter(x => x._1.contains(time)).size > 0)
        .mapVertices{case (vid, attr) => attr.head._2}
        .mapEdges(e => (e.attr._1, e.attr._2.head._2))
    } else
      Graph[VD,(EdgeId, ED)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
  }

  override def coalesce(): OneGraph[VD, ED] = {
    if (coalesced)
      this
    else { //no need to rebuild the graph from rdds
      val newgs = graphs.mapVertices{ case (vid, attr) => TempGraphOps.coalesceIntervals(attr.toList).toArray}.mapEdges(e => (e.attr._1, TempGraphOps.coalesceIntervals(e.attr._2.toList).toArray))
      implicit val ord = TempGraphOps.dateOrdering
      val newIntvs = OneGraph.computeIntervals(newgs)
      new OneGraph(newIntvs, newgs, defaultValue, storageLevel, true)
    }
  }

  override def slice(bound: Interval): OneGraph[VD, ED] = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this
    if (!span.intersects(bound)) {
      return emptyGraph[VD,ED](defaultValue)
    }

    val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
    val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end
    val selectBound:Interval = Interval(startBound, endBound)
    //compute indices of start and stop
    val selectStart:Int = intervals.indexWhere(ii => ii.intersects(selectBound))
    val selectStop:Int = intervals.lastIndexWhere(ii => ii.intersects(selectBound))

    val newIntvs: Array[Interval] = intervals.slice(selectStart, selectStop+1).map(intv => if (intv.start.isBefore(startBound) || intv.end.isAfter(endBound)) intv.intersection(selectBound).get else intv)

    val newgs = graphs.mapVertices{
      case (vid, attr) => attr.flatMap{
        case (intv, aa) => intv.intersection(selectBound).map(xx => (xx, aa))}
    }.mapEdges(e => (e.attr._1, e.attr._2.flatMap{
      case (intv, aa) => intv.intersection(selectBound).map(xx => (xx, aa))})
    ).subgraph(vpred = (vid, attr) => attr.size > 0, epred = e => e.attr._2.size > 0)

    //slice does not change coalesce, so if we have eager coalesce, this will stay coalesced
    //and if we don't, this is still right
    new OneGraph(newIntvs, newgs, defaultValue, storageLevel, coalesced)
  }

  override def vsubgraph(pred: (VertexId, VD,Interval) => Boolean): OneGraph[VD,ED] = {
    val coal = coalesced //to avoid serialization issues
    val newgs = graphs.mapVertices{ 
      case (vid, attr) => if (coal) attr.filter{ case (intv, aa) => pred(vid, aa, intv)} else TempGraphOps.coalesceIntervals(attr.toList).toArray.filter{ case (intv, aa) => pred(vid, aa, intv)}
    }.subgraph(vpred = (vid, attr) => attr.size > 0)
    //need to constrain the edges to be within the intervals of their new vertices
    //because subgraph above only takes out edges for which vertices went away completely
      .mapTriplets( ept => (ept.attr._1, constrainEdges(ept.attr._2, ept.srcAttr, ept.dstAttr)))
      .subgraph(epred = e => e.attr._2.size > 0)

    //have to compute new intervals because we potentially don't cover as much
    val newIntvs = OneGraph.computeIntervals[VD,ED](newgs)
    //don't need to check eager coalesce since subgraph does not change coalesced status
    new OneGraph(newIntvs, newgs, defaultValue, storageLevel, coalesced)
  }

  override def esubgraph(pred: TEdgeTriplet[VD,ED] => Boolean, tripletFields: TripletFields): OneGraph[VD,ED] = {
    val coal = coalesced
    val newgs = graphs.mapTriplets{e => 
      val et = new TEdgeTriplet[VD, ED]
      et.eId = e.attr._1
      et.srcId = e.srcId
      et.dstId = e.dstId
      val eattr = if (coal) e.attr._2 else TempGraphOps.coalesceIntervals(e.attr._2.toList).toArray
      if (tripletFields == TripletFields.None || tripletFields == TripletFields.EdgeOnly) {
        (e.attr._1, eattr.filter{ case (intv, aa) =>
          et.attr = aa
          et.interval = intv
          pred(et)
        })
      } else {
        //it is possible for the edge to correspond to several
        //end-point values so we need to break it up then
        (e.attr._1, eattr.flatMap{ case (intv, aa) =>
          //FIXME? should we also assure coalescing of srcAttr and dstAttr?
          val allSrc = e.srcAttr.filter(ii => ii._1.intersects(intv))
          val allDst = e.dstAttr.filter(ii => ii._1.intersects(intv))
          val all = for {
            i <- allSrc
            j <- allDst
            if i._1.intersects(j._1)
          } yield (i, j)
          all.flatMap { case (src, dst) =>
            et.attr = aa
            et.srcAttr = src._2
            et.dstAttr = dst._2
            val ii = intv.intersection(src._1).get.intersection(dst._1).get
            et.interval = ii
            if (pred(et)) Some((ii, aa)) else None
          }
        })
      }
    }.subgraph(epred = e => e.attr._2.size > 0)

    //do not need to check eager coalesce because subgraph does not change it
    new OneGraph(OneGraph.computeIntervals(newgs), newgs, defaultValue, storageLevel, coalesced)
  }

  override def createAttributeNodes(vAggFunc: (VD, VD) => VD)(vgroupby: (VertexId, VD) => VertexId): OneGraph[VD, ED] = {
    //have to make a new graph
    val newvs = graphs.vertices.flatMap{ case (vid, attr) =>
      attr.map{ case (intv, aa) => (vgroupby(vid, aa), (intv, aa))}
    }
    val newes = graphs.triplets.flatMap(et =>
      et.attr._2.flatMap{ case (intv, aa) => 
        //we need to get all the triplets because the value of end point attr
        //might change during one edge tuple
        val srcAttrs = et.srcAttr.filter{ case (ii, bb) => ii.intersects(intv)}
        val dstAttrs = et.dstAttr.filter{ case (ii, cc) => ii.intersects(intv)}
        for {
          //this will create all possible combinations
          x <- srcAttrs; y <- dstAttrs 
          if x._1.intersects(y._1)
        } yield ((et.attr._1, vgroupby(et.srcId, x._2), vgroupby(et.dstId, y._2)), Array((intv.intersection(x._1).get.intersection(y._1).get, aa)))
      }
    )

    val combOpV = TempGraphOps.mergeIntervalLists(vAggFunc, _: List[(Interval,VD)], _: List[(Interval,VD)])
    val newgs: Graph[Array[(Interval,VD)], (EdgeId, Array[(Interval,ED)])] = Graph(newvs.aggregateByKey(List[(Interval,VD)]())(seqOp = (u: List[(Interval,VD)], v: (Interval,VD)) => combOpV(u, List[(Interval,VD)](v)), combOpV).mapValues(_.toArray),
      newes.reduceByKey((a,b) => a ++ b).map(e => Edge(e._1._2, e._1._3, (e._1._1, e._2))),
      Array[(Interval,VD)](), storageLevel, storageLevel)

    //intervals don't change but the result is uncoalesced
    if (ProgramContext.eagerCoalesce)
      new OneGraph(intervals, newgs.mapVertices{ case (vid, attr) => TempGraphOps.coalesceIntervals(attr.toList).toArray}.mapEdges(e => (e.attr._1, TempGraphOps.coalesceIntervals(e.attr._2.toList).toArray)), defaultValue, storageLevel, true)
    else
      new OneGraph(intervals, newgs, defaultValue, storageLevel, false)
  }

  override protected def aggregateByChange(c: ChangeSpec, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): OneGraph[VD, ED] = {
    val size: Integer = c.num
    val newIntvs = intervals.grouped(size).map(grp => Interval(grp(0).start, grp.last.end)).toArray
    val newIntvsb = ProgramContext.sc.broadcast(newIntvs)

    val split: (Interval => Array[(Interval, Interval)]) = (interval: Interval) => {
      newIntvsb.value.flatMap{ intv =>
        val res = intv.intersection(interval)
        if (res.isEmpty)
          None
        else
          Some(intv, res.get)
      }
    }

    val filtered = graphs.mapVertices { (vid, attr) =>
      attr.flatMap{ case (intv, aa) =>
        split(intv).map(ii => (ii._1, (ii._2.ratio(ii._1), aa)))
      }.groupBy(_._1)
        .mapValues(x => x.map(_._2).reduce((a,b) => (a._1 + b._1, vAggFunc(a._2, b._2))))
        .toArray
        .filter(x => vquant.keep(x._2._1))
        .map(x => (x._1, x._2._2))
    }.subgraph(vpred = (vid, attr) => attr.size > 0)
      .mapTriplets{ept =>
      (ept.attr._1, ept.attr._2.flatMap{ case (intv, aa) =>
        split(intv).map(ii => (ii._1, (ii._2.ratio(ii._1), aa)))
      }.groupBy(_._1)
        .mapValues(x => x.map(_._2).reduce((a,b) => (a._1 + b._1, eAggFunc(a._2, b._2))))
        .toArray
        .filter(x => equant.keep(x._2._1))
        .map(x => (x._1, x._2._2))
      //now need to constrain - simplified because the intervals are exactly the same
        .filter{ case (intv, aa) =>
          ept.srcAttr.find(ii => ii._1.intersects(intv)).isDefined && ept.dstAttr.find(ii => ii._1.intersects(intv)).isDefined
      })
    }.subgraph(epred = et => et.attr._2.size > 0)

    if (ProgramContext.eagerCoalesce)
      new OneGraph(newIntvs, filtered.mapVertices{ case (vid, attr) => TempGraphOps.coalesceIntervals(attr.toList).toArray}.mapEdges(e => (e.attr._1, TempGraphOps.coalesceIntervals(e.attr._2.toList).toArray)), defaultValue, storageLevel, true)
    else
      new OneGraph(newIntvs, filtered, defaultValue, storageLevel, false)
    
  }

  override protected def aggregateByTime(c: TimeSpec, vquant: Quantification, equant: Quantification, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): OneGraph[VD, ED] = {
    val start = span.start
    val newIntvs = span.split(c.res, start).map(_._2).reverse.toArray

    val filtered = graphs.mapVertices { (vid, attr) =>
      attr.flatMap{ case (intv, aa) =>
        intv.split(c.res, start).map(ii => (ii._2, (ii._1.ratio(ii._2), aa)))
      }.groupBy(_._1)
        .mapValues(x => x.map(_._2).reduce((a,b) => (a._1 + b._1, vAggFunc(a._2, b._2))))
        .toArray
        .filter(x => vquant.keep(x._2._1))
        .map(x => (x._1, x._2._2))
    }.subgraph(vpred = (vid, attr) => attr.size > 0)
      .mapTriplets{ept =>
      (ept.attr._1, ept.attr._2.flatMap{ case (intv, aa) =>
        intv.split(c.res, start).map(ii => (ii._2, (ii._1.ratio(ii._2), aa)))
      }.groupBy(_._1)
        .mapValues(x => x.map(_._2).reduce((a,b) => (a._1 + b._1, eAggFunc(a._2, b._2))))
        .toArray
        .filter(x => equant.keep(x._2._1))
        .map(x => (x._1, x._2._2))
      //now need to constrain - simplified because the intervals are exactly the same
        .filter{ case (intv, aa) =>
          ept.srcAttr.find(ii => ii._1.intersects(intv)).isDefined && ept.dstAttr.find(ii => ii._1.intersects(intv)).isDefined
      })
    }.subgraph(epred = et => et.attr._2.size > 0)

    if (ProgramContext.eagerCoalesce)
      new OneGraph(newIntvs, filtered.mapVertices{ case (vid, attr) => TempGraphOps.coalesceIntervals(attr.toList).toArray}.mapEdges(e => (e.attr._1, TempGraphOps.coalesceIntervals(e.attr._2.toList).toArray)), defaultValue, storageLevel, true)
    else
      new OneGraph(newIntvs, filtered, defaultValue, storageLevel, false)

  }

  override def vmap[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2, defVal: VD2)(implicit eq: VD =:= VD2 = null): OneGraph[VD2, ED] = {
    //FIXME? should we coalesce vertices since the interval is passed into the function?
    val newgs = graphs.mapVertices{ case (vid, attr) =>
      attr.map{ case (intv, aa) => (intv, map(vid, intv, aa))
      }
    }

    if (ProgramContext.eagerCoalesce) {
      val g = newgs.mapVertices{ case (vid, attr) => TempGraphOps.coalesceIntervals(attr.toList).toArray}
      new OneGraph(OneGraph.computeIntervals(g), g, defVal, storageLevel, true)
    } else
      new OneGraph(intervals, newgs, defVal, storageLevel, false)
  }

  override def emap[ED2: ClassTag](map: TEdge[ED] => ED2): OneGraph[VD, ED2] = {
    //TODO: this creates a lot of unnecessary objects which will make it slow
    //because of garbage collection. rewrite.
    //FIXME? should we coalesce vertices and edges since the interval may be part of the map function
    val newgs = graphs.mapEdges(e =>
      (e.attr._1, e.attr._2.map{ case (intv, aa) => 
        (intv, map(TEdge(e.attr._1, e.srcId, e.dstId, intv, aa)))
      })
    )

    if (ProgramContext.eagerCoalesce) {
      val g = newgs.mapEdges(e => (e.attr._1, TempGraphOps.coalesceIntervals(e.attr._2.toList).toArray))
      new OneGraph(OneGraph.computeIntervals(g), g, defaultValue, storageLevel, true)
    } else
      new OneGraph(intervals, newgs, defaultValue, storageLevel, false)
  }

  override def union(other: TGraphNoSchema[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): OneGraph[VD, ED] = {
    //union is correct whether the two input graphs are coalesced or not
    var grp2: OneGraph[VD, ED] = other match {
      case grph: OneGraph[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    //compute new intervals
    implicit val ord = TempGraphOps.dateOrdering
    val newIntvs: Array[Interval] = intervals.flatMap(ii => Seq(ii.start, ii.end)).union(grp2.intervals.flatMap(ii => Seq(ii.start, ii.end))).distinct.sortBy(c => c).sliding(2).map(x => Interval(x(0), x(1))).toArray

    val newgs = Graph(graphs.vertices.union(grp2.graphs.vertices).reduceByKey((a,b) => TempGraphOps.mergeIntervalLists[VD](vFunc, a.toList, b.toList).toArray),
      graphs.edges.union(grp2.graphs.edges)
        .map(e => ((e.attr._1, e.srcId, e.dstId), e.attr._2)).reduceByKey((a,b) => TempGraphOps.mergeIntervalLists[ED](eFunc, a.toList, b.toList).toArray).map(e => Edge(e._1._2, e._1._3, (e._1._1, e._2))),
      Array[(Interval,VD)](), storageLevel, storageLevel)

    new OneGraph(newIntvs, newgs, defaultValue, storageLevel, false)
  }

  override def difference(other: TGraphNoSchema[VD, ED]): OneGraph[VD,ED] = {
    var grp2: OneGraph[VD, ED] = other match {
      case grph: OneGraph[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (span.intersects(grp2.span)) {
      //compute new intervals
      implicit val ord = TempGraphOps.dateOrdering
      val newIntvs: Array[Interval] = intervals.flatMap(ii => Seq(ii.start, ii.end)).union(grp2.intervals.flatMap(ii => Seq(ii.start, ii.end)).filter(ii => span.contains(ii))).distinct.sortBy(c => c).sliding(2).map(x => Interval(x(0), x(1))).toArray

      //if a vertex is only in left side, keep it
      //if present in both, keep only non-overlaps
      val newgs = graphs.outerJoinVertices(grp2.graphs.vertices)((vid, attr1, attr2) =>
        if (attr2.isDefined) {
          //attribute value does not matter, as long as the interval overlaps
          attr1.flatMap{ case (intv, aa) =>
            val other = attr2.get.filter(ii => ii._1.intersects(intv)).sortBy(c => c._1)
            if (other.size > 0)
              intv.difference(Interval(other.head._1.start,other.last._1.end)).map(ii => (ii,aa))
            else
              Seq((intv, aa))
          }
        } else attr1
      ).subgraph(vpred = (vid, attr) => attr.size > 0)
      //constrain the edges for vertices that went away
        .mapTriplets(ept => (ept.attr._1, constrainEdges(ept.attr._2, ept.srcAttr, ept.dstAttr)))
        .subgraph(epred = et => et.attr._2.size > 0)

      new OneGraph(newIntvs, newgs, defaultValue, storageLevel, coalesced && grp2.coalesced)
    } else {
      this
    }
  }

  override def intersection(other: TGraphNoSchema[VD, ED] , vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): OneGraph[VD, ED] = {
    var grp2: OneGraph[VD, ED] = other match {
      case grph: OneGraph[VD, ED] => grph
      case _ => throw new ClassCastException
    }

    if (span.intersects(grp2.span)) {
      //compute new intervals
      val st = TempGraphOps.maxDate(intervals.head.start, grp2.intervals.head.start)
      val en = TempGraphOps.minDate(intervals.last.end, grp2.intervals.last.end)
      val in = Interval(st, en)
      implicit val ord = TempGraphOps.dateOrdering
      val newIntvs: Array[Interval] = intervals.map(ii => ii.start).filter(ii => in.contains(ii)).union(grp2.intervals.map(ii => ii.start).filter(ii => in.contains(ii))).union(Seq(en)).distinct.sortBy(c => c).sliding(2).map(x => Interval(x(0), x(1))).toArray
      
      //TODO: this will be wildly inefficient for a vertex/edge with many tuples
      //because of the cartesian
      val newgs = Graph(graphs.vertices.join(grp2.graphs.vertices).mapValues{ case (a,b) => for {
        x <- a; y <- b
        if x._1.intersects(y._1)
      } yield (x._1.intersection(y._1).get, vFunc(x._2, y._2))
      }.filter(v => v._2.size > 0), 
        graphs.edges.map(e => ((e.attr._1, e.srcId, e.dstId), e.attr._2)).join(grp2.graphs.edges.map(e => ((e.attr._1, e.srcId, e.dstId), e.attr._2)))
          .map{ case (k,v) => Edge(k._2, k._3, (k._1, for { x <- v._1; y <- v._2; if x._1.intersects(y._1) } yield (x._1.intersection(y._1).get, eFunc(x._2, y._2))))}
          .filter(e => e.attr._2.size > 0),
        Array[(Interval,VD)](), storageLevel, storageLevel)

      new OneGraph(newIntvs, newgs, defaultValue, storageLevel, false)

    } else
      emptyGraph(defaultValue)
  }

  /** Analytics */

  override def pregel[A: ClassTag]
  (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
    activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
    sendMsg: EdgeTriplet[VD, (EdgeId, ED)] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A): OneGraph[VD, ED] = {

    //because we run for all time instances at the same time,
    //need to convert programs and messages to the map form
    val initM: Int2ObjectOpenHashMap[A] = {
      val tmp = new Int2ObjectOpenHashMap[A]()
      (0 to intervals.size).map{ ii =>
        tmp.put(ii, initialMsg)
      }
      tmp
    }

    val vertexP = (id: VertexId, attr: Int2ObjectOpenHashMap[VD], msg: Int2ObjectOpenHashMap[A]) => {
      var vals = attr.clone

      val iter = attr.iterator
      while (iter.hasNext) {
        val (index,v) = iter.next
        vals.update(index, vprog(id, v, msg.getOrDefault(index, defValue)))
      }
      vals
    }
  
    val sendMsgC = (edge: EdgeTriplet[Int2ObjectOpenHashMap[VD], (EdgeId, Int2ObjectOpenHashMap[ED])]) => {
      //sendMsg takes in an EdgeTriplet[VD,ED]
      //so we have to construct those for each TimeIndex
      edge.attr._2.toList.flatMap{ case (k,v) =>
        val et = new EdgeTriplet[VD, (EdgeId,ED)]
        et.srcId = edge.srcId
        et.dstId = edge.dstId
        et.srcAttr = edge.srcAttr(k)
        et.dstAttr = edge.dstAttr(k)
        et.attr = (edge.attr._1, v)
        //this returns Iterator[(VertexId, A)], but we need
        //Iterator[(VertexId, Map[TimeIndex, A])]
        sendMsg(et).map{x => 
          val tmp = new Int2ObjectOpenHashMap[A]()
          tmp.put(k, x._2)
          (x._1, tmp)
        }
      }
        .iterator
    }

    val mergeMsgC = (a: Int2ObjectOpenHashMap[A], b: Int2ObjectOpenHashMap[A]) => {
      val itr = a.iterator

      while(itr.hasNext){
        val (index, oldv) = itr.next()
        b.update(index, mergeMsg(oldv, b.getOrDefault(index, defValue)))
      }
      b
    }

    val intvs = ProgramContext.sc.broadcast(intervals)
    val split = (interval: Interval) => {
      intvs.value.zipWithIndex.flatMap{ intv =>
        if (intv._1.intersects(interval))
          Some(intv._2)
        else
          None
      }
    }

    //need to convert vertex and edge attributes into maps
    val grph = graphs.mapVertices{ case (vid, attr) =>
      var tmp = new Int2ObjectOpenHashMap[VD]()
      attr.foreach{ case (intv, aa) => split(intv).foreach{ ii => tmp.put(ii,aa)}}
      tmp
    }.mapEdges{e =>
      var tmp = new Int2ObjectOpenHashMap[ED]()
      e.attr._2.foreach{ case (intv, aa) => split(intv).foreach{ ii => tmp.put(ii,aa)}}
      (e.attr._1, tmp)
    }

    val newgrp = Pregel(grph, initM, maxIterations, activeDirection)(vertexP, sendMsgC, mergeMsgC).mapVertices{ case (vid, mp) =>
      mp.toArray.map{ case (index, aa) => (intvs.value(index),aa)}
    }.mapEdges(e =>
      (e.attr._1, e.attr._2.toArray.map{ case (index, aa) => (intvs.value(index),aa)})
    )

    //now convert back to array
    new OneGraph(intervals, newgrp, defaultValue, storageLevel, false)
    
  }

  override def degree: RDD[(VertexId, (Interval, Int))] = {
    //we need to be able to merge messages with overlapping intervals
    val mergeFunc = TempGraphOps.mergeIntervalLists((a:Int,b:Int) => a + b, _: List[(Interval,Int)], _: List[(Interval,Int)])
    val res = graphs.aggregateMessages[List[(Interval,Int)]](
      ctx => {
        ctx.attr._2.foreach{ii => 
          ctx.sendToSrc(List((ii._1, 1)))
          ctx.sendToDst(List((ii._1, 1)))
        }
      }, mergeFunc, TripletFields.EdgeOnly)
      .flatMap{ case (vid, lst) => lst.map{ case (intv, deg) => (vid, (intv, deg))}}

    TGraphNoSchema.coalesce(res)
  }

  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): OneGraph[(VD, Double), ED] = {
    val undirected = !uni

    val zipped = ProgramContext.sc.broadcast(intervals.zipWithIndex)
    val split: (Interval => BitSet) = (interval: Interval) => {
      BitSet() ++ zipped.value.flatMap( ii => if (interval.intersects(ii._1)) Some(ii._2) else None)
    }

    //convert to bitset
    val bitGraph: Graph[BitSet,(EdgeId, BitSet)] = graphs.mapVertices{ case (vid, attr) =>
      attr.map{ case (intv,aa) => split(intv)}.reduce((a,b) => a union b)
    }.mapEdges(e =>
      (e.attr._1, e.attr._2.map{ case (intv,aa) => split(intv)}.reduce((a,b) => a union b))
    )

    //collect degrees
    val mergeFunc = (a:Int2IntOpenHashMap, b:Int2IntOpenHashMap) => {
      val itr = a.iterator

      while(itr.hasNext){
        val (index, count) = itr.next()
        b.update(index, (count + b.getOrDefault(index, 0)))
      }
      b
    }

    val degrees = bitGraph.aggregateMessages[Int2IntOpenHashMap](
      ctx => {
        ctx.attr._2.foreach{ii =>
          ctx.sendToSrc(new Int2IntOpenHashMap(Array(ii),Array(1)))
          if (undirected) ctx.sendToDst(new Int2IntOpenHashMap(Array(ii),Array(1))) else ctx.sendToDst(new Int2IntOpenHashMap(Array(ii),Array(0)))
        }
      }, mergeFunc, TripletFields.EdgeOnly)

    val pagerankGraph: Graph[Int2ObjectOpenHashMap[(Double,Double)], (EdgeId, Int2ObjectOpenHashMap[(Double,Double)])] = bitGraph.outerJoinVertices(degrees) {
      //convert to time indices and degrees
      case (vid, vdata, Some(deg)) => deg
      case (vid, vdata, None) => new Int2IntOpenHashMap()
    }.mapTriplets{ e =>
      (e.attr._1, new Int2ObjectOpenHashMap[(Double,Double)](e.attr._2.toArray, e.attr._2.toArray.map(x => (1.0/e.srcAttr(x), 1.0/e.dstAttr.getOrDefault(x, 0)))))
    }.mapVertices{ (id,attr) =>
      new Int2ObjectOpenHashMap[(Double,Double)](attr.keySet().toIntArray(), Array.fill(attr.size)((0.0,0.0)))
    }.cache()

    val vertexProgram = (id: VertexId, attr: Int2ObjectOpenHashMap[(Double, Double)], msg: Int2DoubleOpenHashMap) => {
      var vals = attr.clone

      val iter = attr.iterator
      while (iter.hasNext) {
        val (index, v) = iter.next
        val newPr = v._1 + (1.0 - resetProb) * msg.getOrDefault(index, 0.0)
        vals.update(index, (newPr, newPr-v._1))
      }
      vals
    }

    val sendMessage = if (undirected)
        (edge: EdgeTriplet[Int2ObjectOpenHashMap[(Double, Double)], (EdgeId, Int2ObjectOpenHashMap[(Double, Double)])]) => {
          edge.attr._2.toList.flatMap{ case (k,v) =>
            if (edge.srcAttr.apply(k)._2 > tol &&
              edge.dstAttr.apply(k)._2 > tol) {
              Iterator((edge.dstId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.srcAttr.apply(k)._2 * v._1))),
                (edge.srcId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.dstAttr.apply(k)._2 * v._2))))
            } else if (edge.srcAttr.apply(k)._2 > tol) {
              Some((edge.dstId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.srcAttr.apply(k)._2 * v._1))))
            } else if (edge.dstAttr.apply(k)._2 > tol) {
              Some((edge.srcId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.dstAttr.apply(k)._2 * v._2))))
            } else {
              None
            }
          }
            .iterator
        }
        else
      (edge: EdgeTriplet[Int2ObjectOpenHashMap[(Double, Double)], (EdgeId, Int2ObjectOpenHashMap[(Double, Double)])]) => {
        edge.attr._2.toList.flatMap{ case (k,v) =>
          if  (edge.srcAttr.apply(k)._2 > tol) {
            Some((edge.dstId, new Int2DoubleOpenHashMap(Array(k.toInt), Array(edge.srcAttr.apply(k)._2 * v._1))))
          } else {
            None
          }
        }
          .iterator
      }
  
    val messageCombiner = (a: Int2DoubleOpenHashMap, b: Int2DoubleOpenHashMap) => {
      val itr = a.iterator

      while(itr.hasNext){
        val (index, count) = itr.next()
        b.update(index, (count + b.getOrDefault(index, 0.0)))
      }
      b
    }

    // The initial message received by all vertices in PageRank
    //has to be a map from every interval index
    var i:Int = 0
    val initialMessage:Int2DoubleOpenHashMap = new Int2DoubleOpenHashMap((0 until intervals.size).toArray, Array.fill(intervals.size)(resetProb / (1.0-resetProb)))

    val dir = if (undirected) EdgeDirection.Either else EdgeDirection.Out
    val resultGraph: Graph[Map[TimeIndex,(Double,Double)], (EdgeId, Map[TimeIndex,(Double,Double)])] = Pregel(pagerankGraph, initialMessage, numIter, activeDirection = dir)(vertexProgram, sendMessage, messageCombiner)
      .asInstanceOf[Graph[Map[TimeIndex,(Double,Double)], (EdgeId, Map[TimeIndex,(Double,Double)])]]

    //now join the values into old graph
    val newgs = graphs.outerJoinVertices(resultGraph.vertices) {
      case (vid, vdata, Some(prank)) =>
        //prank is a Int2ObjectOpenHashsMap with (Double,Double)
        vdata.flatMap{ case (intv, aa) =>
          //compute all the interval indices that this interval covers
          zipped.value.filter(ii => ii._1.intersects(intv)).map(ii => (ii._1, (aa, prank.getOrDefault(ii._2, (resetProb, 0.0))._1)))
        }
      case (vid, vdata, None) => vdata.map{ case (intv, aa) => (intv, (aa, resetProb))}
    }

    new OneGraph(intervals, newgs, (defaultValue, resetProb), storageLevel, false)
  }

  override def connectedComponents(): OneGraph[(VD, VertexId), ED] = {
    //put vid into each attribute, and indices instead of intervals
    val zipped = ProgramContext.sc.broadcast(intervals.zipWithIndex)
    val split: (Interval => BitSet) = (interval: Interval) => {
      BitSet() ++ zipped.value.flatMap( ii => if (interval.intersects(ii._1)) Some(ii._2) else None)
    }

    val conGraph: Graph[Int2LongOpenHashMap, BitSet] = graphs.mapVertices{ case (vid, attr) =>
      new Int2LongOpenHashMap()
    }.mapEdges(e =>  e.attr._2.map{ case (intv,aa) => split(intv)}.reduce((a,b) => a union b))

    val vertexProgram = (id: VertexId, attr: Int2LongOpenHashMap, msg: Int2LongOpenHashMap) => {
      var vals = attr.clone()

      msg.foreach { x =>
        val (k,v) = x
        vals.update(k, math.min(v, attr.getOrDefault(k, id)))
      }
      vals
    }

    val sendMessage = (edge: EdgeTriplet[Int2LongOpenHashMap, BitSet]) => {
      edge.attr.toList.flatMap{ k =>
        if (edge.srcAttr.getOrDefault(k, edge.srcId) < edge.dstAttr.getOrDefault(k, edge.dstId))
          Some((edge.dstId, new Int2LongOpenHashMap(Array(k), Array(edge.srcAttr.getOrDefault(k, edge.srcId).toLong))))
        else if (edge.srcAttr.getOrDefault(k, edge.srcId) > edge.dstAttr.getOrDefault(k, edge.dstId))
          Some((edge.srcId, new Int2LongOpenHashMap(Array(k), Array(edge.dstAttr.getOrDefault(k, edge.dstId).toLong))))
        else
          None
      }
	.iterator
    }

    val messageCombiner = (a: Int2LongOpenHashMap, b: Int2LongOpenHashMap) => {
      val itr = a.iterator

      while(itr.hasNext){
        val (index, minid) = itr.next()
        b.put(index: Int, math.min(minid, b.getOrDefault(index, Long.MaxValue)))
      }
      b
    }

    val i: Int = 0
    //there is really no reason to send an initial message
    val initialMessage: Int2LongOpenHashMap = new Int2LongOpenHashMap()

    val resultGraph: Graph[Map[TimeIndex, VertexId], BitSet] = Pregel(conGraph, initialMessage, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner).asInstanceOf[Graph[Map[TimeIndex, VertexId], BitSet]]

    val newgs = graphs.outerJoinVertices(resultGraph.vertices) {
      case (vid, vdata, Some(cc)) =>
        //cc is a Int2LongOpenHashsMap
        vdata.flatMap{ case (intv, aa) =>
          //compute all the interval indices that this interval covers
          zipped.value.filter(ii => ii._1.intersects(intv)).map(ii => (ii._1, (aa, cc.getOrDefault(ii._2, vid))))
        }
      //this is unlikely/impossible but left here just in case
      case (vid, vdata, None) => vdata.map{ case (intv, aa) => (intv, (aa,vid))}
    }

    new OneGraph(intervals, newgs, (defaultValue, Long.MaxValue), storageLevel, false)
  }

  override def shortestPaths(uni: Boolean, landmarks: Seq[VertexId]): OneGraph[(VD, Map[VertexId, Int]), ED] = {
    //FIXME
    throw new UnsupportedOperationException("shortest paths not yet implemented")
  }

  override def aggregateMessages[A: ClassTag](sendMsg: TEdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A, defVal: A, tripletFields: TripletFields = TripletFields.All): OneGraph[(VD, A), ED] = {

    val mergeFunc = TempGraphOps.mergeIntervalLists[A](mergeMsg, _:List[(Interval,A)], _:List[(Interval,A)])

    val messages = if (tripletFields == TripletFields.None || tripletFields == TripletFields.EdgeOnly)
      graphs.aggregateMessages[List[(Interval,A)]](
        ctx => {
          //make a single message to send to src and/or dst
          //that covers all intervals there's a message for
          val triplet = new TEdgeTriplet[VD,ED]
          triplet.srcId = ctx.srcId
          triplet.dstId = ctx.dstId
          triplet.eId = ctx.attr._1
          //the messages are (Interval,A) pairs
          //don't bother with vertex attributes since they are not needed
          ctx.attr._2.foreach { x =>
            triplet.interval = x._1
            triplet.attr = x._2
            sendMsg(triplet).foreach {y =>
              if (y._1 == ctx.srcId) ctx.sendToSrc(List[(Interval,A)]((x._1, y._2)))
              else if (y._1 == ctx.dstId) ctx.sendToDst(List[(Interval,A)]((x._1, y._2)))
              else
                throw new IllegalArgumentException("trying to send message to a vertex that is neither a source nor a destination")
            }
          }
        }, mergeFunc, tripletFields)
    else
      graphs.aggregateMessages[List[(Interval,A)]](
        ctx => {
          //make a single message to send to src and/or dst
          //that covers all intervals there's a message for
          val triplet = new TEdgeTriplet[VD,ED]
          triplet.srcId = ctx.srcId
          triplet.dstId = ctx.dstId
          triplet.eId = ctx.attr._1
          //get vertex attributes
          ctx.attr._2.foreach { x =>
            triplet.interval = x._1
            triplet.attr = x._2
            val srcAttrs = ctx.srcAttr.filter(y => y._1.intersects(x._1))
            val dstAttrs = ctx.dstAttr.filter(y => y._1.intersects(x._1))
            val pairs = for {
              s <- srcAttrs; d <- dstAttrs
              if s._1.intersects(d._1)
            } yield (s._2, d._2)
            pairs.foreach { y =>
              triplet.srcAttr = y._1
              triplet.dstAttr = y._2
              sendMsg(triplet).foreach {y =>
                if (y._1 == ctx.srcId) ctx.sendToSrc(List[(Interval,A)]((x._1, y._2)))
                else if (y._1 == ctx.dstId) ctx.sendToDst(List[(Interval,A)]((x._1, y._2)))
                else
                  throw new IllegalArgumentException("trying to send message to a vertex that is neither a source nor a destination")
              }
            }
          }
        }, mergeFunc, tripletFields)

    //now merge. warning: messages are not guaranteed to cover vertex lifetime
    //but are guaranteed to not go outside vertex lifetime
    implicit val ord = TempGraphOps.dateOrdering
    val newgs = graphs.outerJoinVertices(messages) {
      case (vid, olds, Some(news)) =>
        olds.flatMap(i => Seq(i._1.start, i._1.end)).union(news.flatMap(i => Seq(i._1.start, i._1.end))).distinct.sortBy(c => c).sliding(2).map(lst => Interval(lst(0),lst(1))).toArray.map(intv =>
          (intv, (olds.find(i => i._1.intersects(intv)).get._2, news.find(i => i._1.intersects(intv)).getOrElse((intv, defVal))._2)))
        //this is unlikely but possible
      case (vid, olds, None) => olds.map(x => (x._1, (x._2, defVal)))
    }

    new OneGraph(intervals, newgs, (defaultValue, defVal), storageLevel, false)
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    if (graphs.edges.isEmpty)
      0
    else
      graphs.edges.getNumPartitions
  }

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): OneGraph[VD, ED] = {
    graphs.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): OneGraph[VD, ED] = {
    graphs.unpersist(blocking)
    this
  }

  override def partitionBy(tgp: TGraphPartitioning): OneGraph[VD, ED] = {
    if (tgp.pst != PartitionStrategyType.None) {
      partitioning = tgp
      var numParts = if (tgp.parts > 0) tgp.parts else graphs.edges.getNumPartitions
      //not changing the intervals
      new OneGraph[VD, ED](intervals, graphs.partitionByExt(PartitionStrategies.makeStrategy(tgp.pst, 0, intervals.size, tgp.runs), numParts), defaultValue, storageLevel, coalesced)
    } else
      this
  }

  private def constrainEdges(edges: Array[(Interval,ED)], srcVerts: Array[(Interval,VD)], dstVerts: Array[(Interval,VD)]): Array[(Interval,ED)] = {
    //first compute the lifetime of both vertices
    //the edge has to be contained in lifetime of both end points
    val srcLife = TempGraphOps.coalesceIntervals(srcVerts.map(x => (x._1, true)).toList).map(x => x._1)
    val dstLife = TempGraphOps.coalesceIntervals(dstVerts.map(x => (x._1, true)).toList).map(x => x._1)
    //find the interval that intersects, shorten to it
    val emptyI = Interval.empty
    edges.flatMap{ case (intv, aa) =>
      for {
        x <- srcLife.filter(ii => ii.intersects(intv)); y <- dstLife.filter(ii => ii.intersects(intv))
        if x.intersects(y)
      } yield (intv.intersection(x).get.intersection(y).get, aa)
    }
  }

  override protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): OneGraph[V, E] = OneGraph.emptyGraph(defVal)
  override def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[TEdge[E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): OneGraph[V, E] = OneGraph.fromRDDs(verts, edgs, defVal, storLevel, coalesced)

}

object OneGraph {

  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): OneGraph[V,E] = new OneGraph(Array[Interval](), Graph(ProgramContext.sc.emptyRDD[(VertexId, Array[(Interval,V)])], ProgramContext.sc.emptyRDD[Edge[(EdgeId, Array[(Interval,E)])]]), defVal, coal = true)

  def computeIntervals[V: ClassTag, E: ClassTag](graph: Graph[Array[(Interval,V)], (EdgeId, Array[(Interval,E)])]): Array[Interval] = {
    implicit val ord = TempGraphOps.dateOrdering
    graph.vertices.flatMap{ case (vid, attr) => attr.flatMap(xx => Seq(xx._1.start, xx._1.end)).distinct}.union(graph.edges.flatMap(e => e.attr._2.flatMap(xx => Seq(xx._1.start, xx._1.end)).distinct)).distinct.collect.sortBy(c => c).sliding(2).map(lst => Interval(lst(0), lst(1))).toArray
  }

  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[TEdge[E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): OneGraph[V, E] = {
    val cverts = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(verts) else verts
    val cedges = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(edgs.map(e => e.toPaired)).map(e => TEdge(e._1, e._2)) else edgs
    val coal = coalesced | ProgramContext.eagerCoalesce

    val intervals = TGraphNoSchema.computeIntervals(cverts, cedges).collect
    val newgs = Graph[Array[(Interval,V)], (EdgeId, Array[(Interval,E)])](cverts.groupByKey.mapValues(_.toArray),
      cedges.map(te => te.toPaired).groupByKey.map{ case (ids, iter) => Edge(ids._2, ids._3, (ids._1, iter.toArray))},
      Array[(Interval,V)](), storLevel, storLevel)

    new OneGraph(intervals, newgs, defVal, storLevel, coal)

  }

  def fromDataFrames[V: ClassTag, E: ClassTag](verts: org.apache.spark.sql.DataFrame, edgs: org.apache.spark.sql.DataFrame, defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): OneGraph[V, E] = {
    val cverts = verts.rdd.map(r => (r.getLong(0), (Interval(r.getLong(1), r.getLong(2)), r.getAs[V](3))))
    val cedgs = edgs.rdd.map(r => ((r.getLong(0), r.getLong(1), r.getLong(2)), (Interval(r.getLong(3), r.getLong(4)), r.getAs[E](5))))

    val cov = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(cverts) else cverts
    val coe = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(cedgs) else cedgs
    val coal = coalesced | ProgramContext.eagerCoalesce

    val intervals = TGraphNoSchema.computeIntervals(cov, coe.map(e => TEdge(e._1, e._2))).collect
    val newgs = Graph[Array[(Interval,V)], (EdgeId, Array[(Interval,E)])](cov.groupByKey.mapValues(_.toArray),
      coe.groupByKey.map{ case (ids, iter) => Edge(ids._2, ids._3, (ids._1, iter.toArray))},
      Array[(Interval,V)](), storLevel, storLevel)

    new OneGraph(intervals, newgs, defVal, storLevel, coal)
  }
}
