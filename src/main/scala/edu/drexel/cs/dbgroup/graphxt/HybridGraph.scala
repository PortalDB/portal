package edu.drexel.cs.dbgroup.graphxt

import scala.collection.parallel.ParSeq
import scala.collection.immutable.BitSet
import scala.collection.mutable.LinkedHashMap
import scala.collection.breakOut

import scala.reflect.ClassTag
import scala.util.control._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.HashPartitioner

import org.apache.spark.graphx._
import org.apache.spark.rdd._

import edu.drexel.cs.dbgroup.graphxt.util.MultifileLoad

import java.time.LocalDate

class HybridGraph[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], runs: Seq[Int], gps: ParSeq[Graph[BitSet, BitSet]], veratts: RDD[(VertexId,(TimeIndex,VD))], edgatts: RDD[((VertexId,VertexId),(TimeIndex,ED))]) extends TemporalGraph[VD, ED] with Serializable {
  val graphs: ParSeq[Graph[BitSet, BitSet]] = gps
  val resolution: Resolution = if (intvs.size > 0) intvs.head.resolution else Resolution.zero
  //this is how many consecutive intervals are in each aggregated graph
  val widths: Seq[Int] = runs
  
  if (widths.reduce(_ + _) != intvs.size)
    throw new IllegalArgumentException("temporal sequence and runs do not match")

  intvs.foreach { ii =>
    if (!ii.resolution.isEqual(resolution))
      throw new IllegalArgumentException("temporal sequence is not valid, intervals are not all of equal resolution")
  }

  val intervals: Seq[Interval] = intvs
  lazy val span: Interval = if (intervals.size > 0) Interval(intervals.head.start, intervals.last.end) else Interval(LocalDate.now, LocalDate.now)

  //vertex attributes are kept in a separate rdd with an id key
  //so there are multiple entries for the same key potentially
  val vertexattrs: RDD[(VertexId,(TimeIndex,VD))] = veratts

  //edge attributes are kept in a separate rdd with an id,id key
  val edgeattrs: RDD[((VertexId,VertexId),(TimeIndex,ED))] = edgatts

  /** Default constructor is provided to support serialization */
  protected def this() = this(Seq[Interval](), Seq[Int](0), ParSeq[Graph[BitSet,BitSet]](), ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)

  override def size(): Int = intervals.size

  override def materialize() = {
    graphs.foreach { x =>
      if (!x.edges.isEmpty)
        x.numEdges
      if (!x.vertices.isEmpty)
        x.numVertices
    }
    vertexattrs.count
    edgeattrs.count
  }

  override def vertices: RDD[(VertexId,Map[Interval, VD])] = {
    val start = span.start
    val res = resolution

    vertexattrs.map{ case (k, v) => (k, Map[Interval,VD](res.getInterval(start, v._1) -> v._2))}.reduceByKey{case (a,b) => a ++ b}
  }

  override def verticesFlat: RDD[(VertexId, (Interval, VD))] = {
    val start = span.start
    val res = resolution

    vertexattrs.map{ case (k,v) => (k, (res.getInterval(start, v._1), v._2))}
  }

  override def edges: RDD[((VertexId,VertexId),Map[Interval, ED])] = {
    val start = span.start
    val res = resolution

    edgeattrs.map{ case (k,v) => (k, Map[Interval,ED](res.getInterval(start, v._1) -> v._2))}
      .reduceByKey((a: Map[Interval, ED], b: Map[Interval, ED]) => a ++ b)
  }

  override def edgesFlat: RDD[((VertexId,VertexId),(Interval, ED))] = {
    val start = span.start
    val res = resolution

    edgeattrs.map{ case (k,v) => (k, (res.getInterval(start, v._1), v._2))}
  }

  override def degrees: RDD[(VertexId,Map[Interval, Int])] = {
    val start = span.start
    val allgs = graphs.map(Degree.run(_))
    allgs.map(g => g.vertices).reduce((x,y) => VertexRDD(x union y))
      .reduceByKey((a: Map[TimeIndex, Int], b: Map[TimeIndex, Int]) => a ++ b).map{ case (vid,vattr) => (vid, vattr.map{ case (k,v) => (resolution.getInterval(start, k),v)})}
  }

  override def getTemporalSequence: Seq[Interval] = intervals

  override def getSnapshot(period: Interval): Graph[VD, ED] = {
    val index = intervals.indexOf(period)
    if (index >= 0) {
      //the index of the aggregated graph is based on the width
      val filteredvas: RDD[(VertexId, VD)] = vertexattrs.filter{ case (k,v) => v._1 == index}.map{ case (k,v) => (k, v._2)}
      val filterededs: RDD[Edge[ED]] = edgeattrs.filter{ case (k,v) => v._1 == index}.map{ case (k,v) => Edge(k._1, k._2, v._2)}
      Graph[VD,ED](filteredvas, EdgeRDD.fromEdges[ED,VD](filterededs))
    } else
      Graph[VD,ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
  }

  /** Query operations */
  
  override def select(bound: Interval): TemporalGraph[VD, ED] = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this

    if (span.intersects(bound)) {
      val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
      val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end
      val start = span.start

      //compute indices of start and stop
      //start is inclusive, stop exclusive
      val selectStart:Int = intervals.indexOf(resolution.getInterval(startBound))
      var selectStop:Int = intervals.indexOf(resolution.getInterval(endBound))
      if (selectStop < 0) selectStop = intervals.size
      val newIntvs: Seq[Interval] = intervals.slice(selectStart, selectStop)

      //compute indices of the aggregates that should be included
      //both inclusive
      val partialSum: Seq[Int] = widths.scanLeft(0)(_ + _).tail
      val indexStart: Int = partialSum.indexWhere(_ >= (selectStart+1))
      val indexStop: Int = partialSum.indexWhere(_ >= (selectStop))

      //TODO: rewrite simpler
      val tail: Seq[Int] = if (indexStop == indexStart) Seq() else Seq(widths(indexStop) - partialSum(indexStop) + selectStop)
      val runs: Seq[Int] = Seq(partialSum(indexStart) - selectStart) ++ widths.slice(indexStart, indexStop).drop(1) ++ tail
      val stop1: Int = partialSum(indexStart) - 1
      val stop2: Int = partialSum(indexStop-1)

      //filter out aggregates where we need only partials
      //drop those we don't need
      //keep the rest
      val subg:ParSeq[Graph[BitSet,BitSet]] = graphs.zipWithIndex.flatMap{ case (g,index) =>
        if (index == indexStart || index == indexStop) {
          val mask: BitSet = if (index == indexStart) BitSet((selectStart to stop1): _*) else BitSet((stop2 to (selectStop-1)): _*)
          Some(g.subgraph(
            vpred = (vid, attr) => !(attr & mask).isEmpty,
            epred = et => !(et.attr & mask).isEmpty)
            .mapVertices((vid, vattr) => vattr.filter(x => x >= selectStart && x < selectStop).map(_ - selectStart)).mapEdges(e => e.attr.filter(x => x > selectStart && x < selectStop).map(_ - selectStart)))
        } else if (index > indexStart && index < indexStop) {
          Some(g.mapVertices((vid, vattr) => vattr.map(_ - selectStart))
            .mapEdges(e => e.attr.map(_ - selectStart)))
        } else
          None
      }

      //now need to update the vertex attribute rdd and edge attr rdd
      val vattrs = vertexattrs.filter{ case (k,v) => v._1 >= selectStart && v._1 <= selectStop}.map{ case (k,v) => (k, (v._1 - selectStart, v._2))}
      val eattrs = edgeattrs.filter{ case (k,v) => v._1 >= selectStart && v._1 <= selectStop}.map{ case (k,v) => (k, (v._1 - selectStart, v._2))}

      new HybridGraph[VD, ED](newIntvs, runs, subg, vattrs, eattrs)

    } else
      HybridGraph.emptyGraph[VD,ED]()
  }

  override def select(tpred: Interval => Boolean): TemporalGraph[VD, ED] = {
    val start = span.start
    val res = resolution

    val subg:ParSeq[Graph[BitSet,BitSet]] = graphs.map{ g =>
      g.mapVertices((vid, vattr) => vattr.filter(x => tpred(res.getInterval(start, x))))
      .mapEdges(e => e.attr.filter(x => tpred(res.getInterval(start, x))))
      .subgraph(vpred = (vid, attr) => !attr.isEmpty,
        epred = et => !et.attr.isEmpty)
    }

    new HybridGraph[VD, ED](intervals, widths, subg, 
      vertexattrs.filter{ case (k,v) => tpred(res.getInterval(start, v._1))},
      edgeattrs.filter{ case (k,v) => tpred(res.getInterval(start, v._1))})
  }

  override def select(epred: EdgeTriplet[VD, ED] => Boolean, vpred: (VertexId, VD) => Boolean): TemporalGraph[VD, ED] = {
    throw new UnsupportedOperationException("select not yet impelemented")
  }

  override def aggregate(res: Resolution, sem: AggregateSemantics.Value, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    if (!resolution.isCompatible(res)) {
      throw new IllegalArgumentException("incompatible resolution")
    }

    var intvs: Seq[Interval] = scala.collection.immutable.Seq[Interval]()
    //it is possible that different number of graphs end up in different intervals
    //such as going from days to months
    var index:Int = 0
    val indMap:scala.collection.mutable.ListBuffer[TimeIndex] = scala.collection.mutable.ListBuffer[TimeIndex]()
    //make a map of old indices to new ones
    var counts:scala.collection.immutable.Seq[Int] = scala.collection.immutable.Seq[Int]()

    while (index < intervals.size) {
      val intv:Interval = intervals(index)
      //need to compute the interval start and end based on resolution new units
      val newIntv:Interval = res.getInterval(intv.start)
      val expected:Int = resolution.getNumParts(res, intv.start)

      indMap.insert(index, intvs.size)
      index += 1

      //grab all the intervals that fit within
      val loop = new Breaks
      loop.breakable {
        while (index < intervals.size) {
          val intv2:Interval = intervals(index)
          if (newIntv.contains(intv2)) {
            indMap.insert(index, intvs.size)
            index += 1
          } else {
            loop.break
          }
        }
      }

      counts = counts :+ expected
      intvs = intvs :+ newIntv
    }

    //need to combine graphs such that new intervals don't span graph boundaries
    //at worst this will lead to a single "OneGraph"
    var xx:Int = 0
    var yy:Int = 0
    val countsSum: Seq[Int] = counts.scanLeft(0)(_ + _).tail
    val runsSum: Seq[Int] = widths.scanLeft(0)(_ + _).tail
    var runs: Seq[Int] = Seq[Int]()

    var gps: ParSeq[Graph[BitSet,BitSet]] = ParSeq[Graph[BitSet,BitSet]]()
    //there is no union of two graphs in graphx
    var firstVRDD: RDD[(VertexId, BitSet)] = ProgramContext.sc.emptyRDD
    var firstERDD: RDD[Edge[BitSet]] = ProgramContext.sc.emptyRDD
    var startx:Int = 0
    var numagg:Int = 0

    while (xx < countsSum.size && yy < runsSum.size) {
      if (yy == (runsSum.size - 1) || countsSum(xx) == runsSum(yy)) {
        firstVRDD = firstVRDD.union(graphs(yy).vertices)
        firstERDD = firstERDD.union(graphs(yy).edges)
        numagg = numagg + 1

        val parts:Seq[(Int,Int,Int)] = (startx to xx).map(p => (countsSum.lift(p-1).getOrElse(0), countsSum(p)-1, p))
        if (numagg > 1) {
          firstVRDD = firstVRDD.reduceByKey(_ ++ _)
          firstERDD = firstERDD.map{e => ((e.srcId, e.dstId), e.attr)}.reduceByKey(_ ++ _).map(x => Edge(x._1._1, x._1._2, x._2))
        }
        gps = gps :+ Graph(VertexRDD(firstVRDD.mapValues{ attr =>
          BitSet() ++ parts.flatMap { case (start, end, index) =>
            val mask = BitSet((start to end): _*)
            if (sem == AggregateSemantics.Universal) {
              if (mask.subsetOf(attr))
                Some(index)
              else
                None
            } else if (sem == AggregateSemantics.Existential) {
              if (!(mask & attr).isEmpty)
                Some(index)
              else
                None
            } else None
          }
        }.filter{ case (vid, attr) => !attr.isEmpty}), EdgeRDD.fromEdges[BitSet,BitSet](firstERDD).mapValues{e => 
          BitSet() ++ parts.flatMap { case (start, end, index) =>
            val mask = BitSet((start to end): _*)
            if (sem == AggregateSemantics.Universal) {
              if (mask.subsetOf(e.attr))
                Some(index)
              else
                None
            } else if (sem == AggregateSemantics.Existential) {
              if (!(mask & e.attr).isEmpty)
                Some(index)
              else
                None
            } else None
          }
        }.filter{ e => !e.attr.isEmpty})
        //reset, move on
        firstVRDD = ProgramContext.sc.emptyRDD
        firstERDD = ProgramContext.sc.emptyRDD
        xx = xx+1
        yy = yy+1
        startx = xx
        numagg = 0
        //the number of snapshots in this new aggregate
        runs = runs :+ parts.size
      } else if (countsSum(xx) < runsSum(yy)) {
        xx = xx+1
      } else { //runsSum(y) < countsSum(x)
        firstVRDD = firstVRDD.union(graphs(yy).vertices)
        firstERDD = firstERDD.union(graphs(yy).edges)
        yy = yy+1
        numagg = numagg + 1
      }
    }

    //TODO: do this more efficiently
    val broadcastIndMap = ProgramContext.sc.broadcast(indMap.toSeq)
    val aveReductFactor = counts.head
    val vattrs = if (sem == AggregateSemantics.Universal) vertexattrs.map{ case (k,v) => ((k, broadcastIndMap.value(v._1)), (v._2, 1))}.reduceByKey((x,y) => (vAggFunc(x._1, y._1), x._2 + y._2)).filter{ case (k, (attr,cnt)) => cnt == counts(k._2)}.map{ case (k,v) => (k._1, (k._2, v._1))} else vertexattrs.map{ case (k,v) => ((k, broadcastIndMap.value(v._1)), v._2)}.reduceByKey(vAggFunc, vertexattrs.partitions.size / aveReductFactor).map{ case (k,v) => (k._1, (k._2, v))}
    //FIXME: this does not filter out edges for which vertices no longer exist
    //such as would happen when vertex semantics is universal
    //and edges semantics existential
    val eattrs = if (sem == AggregateSemantics.Universal) 
      edgeattrs.map{ case (k,v) => ((k._1, k._2, broadcastIndMap.value(v._1)), (v._2, 1))}.reduceByKey((x,y) => (eAggFunc(x._1, y._1), x._2 + y._2)).filter{ case (k, (attr,cnt)) => cnt == counts(k._3)}.map{ case (k,v) => ((k._1, k._2), (k._3, v._1))}
    else 
      edgeattrs.map{ case (k,v) => ((k._1, k._2, broadcastIndMap.value(v._1)), v._2)}.reduceByKey(eAggFunc, edgeattrs.partitions.size / aveReductFactor).map{ case (k,v) => ((k._1, k._2), (k._3, v))}

    new HybridGraph[VD, ED](intvs, runs, gps, vattrs, eattrs)
  }

  override def transform[ED2: ClassTag, VD2: ClassTag](emap: (Edge[ED], Interval) => ED2, vmap: (VertexId, Interval, VD) => VD2): TemporalGraph[VD2, ED2] = {
    val start = span.start
    new HybridGraph[VD2, ED2](intervals, widths, graphs, vertexattrs.map{ case (k,v) => (k, (v._1, vmap(k, resolution.getInterval(start, v._1), v._2)))}, edgeattrs.map{ case (k,v) => (k, (v._1, emap(Edge(k._1, k._2, v._2), resolution.getInterval(start, v._1))))})
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val start = span.start
    val res = resolution
    new HybridGraph[VD2, ED](intervals, widths, graphs, vertexattrs.map{ case (k,v) => (k, (v._1, map(k, res.getInterval(start, v._1), v._2)))}, edgeattrs)
  }

  override def mapEdges[ED2: ClassTag](map: (Edge[ED], Interval) => ED2): TemporalGraph[VD, ED2] = {
    val start = span.start
    val res = resolution
    new HybridGraph[VD, ED2](intervals, widths, graphs, vertexattrs, edgeattrs.map{ case (k,v) => (k, (v._1, map(Edge(k._1, k._2, v._2), res.getInterval(start, v._1))))})
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, Map[Interval, U])])(mapFunc: (VertexId, Interval, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val start = span.start

    val in: RDD[((VertexId,TimeIndex),U)] = other.flatMap(x => x._2.map(y => ((x._1, resolution.numBetween(start, y._1.start)), y._2)))

    new HybridGraph[VD2, ED](intervals, widths, graphs, vertexattrs.map{ case (k,v) => ((k, v._1), v._2)}.leftOuterJoin(in).map{ case (k,v) => (k._1, (k._2, mapFunc(k._1, resolution.getInterval(start, k._2), v._1, v._2)))}, edgeattrs)
  }

  override def union(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    throw new UnsupportedOperationException("union not yet implemented")
  }

  override def intersection(other: TemporalGraph[VD, ED], sem: AggregateSemantics.Value, vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    throw new UnsupportedOperationException("intersection not yet implemented")
  }

  override def pregel[A: ClassTag]
  (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
    activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
    sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
    mergeMsg: (A, A) => A): TemporalGraph[VD, ED] = {
    throw new UnsupportedOperationException("pregel not yet implemented")
  }

  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): TemporalGraph[Double, Double] = {
    val runSums = widths.scanLeft(0)(_ + _).tail

    if (uni) {
      def prank(grp: Graph[BitSet,BitSet], minIndex: Int, maxIndex: Int): Graph[LinkedHashMap[TimeIndex,(Double,Double)], LinkedHashMap[TimeIndex,(Double,Double)]] = {
        if (grp.edges.isEmpty)
          Graph[LinkedHashMap[TimeIndex,(Double,Double)],LinkedHashMap[TimeIndex,(Double,Double)]](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
        else {
          UndirectedPageRank.runHybrid(grp, minIndex, maxIndex-1, tol, resetProb, numIter)
        }
      }
    
      var allgs:ParSeq[Graph[LinkedHashMap[TimeIndex,(Double,Double)], LinkedHashMap[TimeIndex,(Double,Double)]]] = graphs.zipWithIndex.map{ case (g,i) => prank(g, runSums.lift(i-1).getOrElse(0), runSums(i))}

      //now extract values
      val vattrs= allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.toSeq.map{ case (k,v) => (vid, (k, v._1))}}}.reduce(_ union _)
      val eattrs = allgs.map{ g => g.edges.flatMap{ e => e.attr.toSeq.map{ case (k,v) => ((e.srcId, e.dstId), (k, v._1))}}}.reduce(_ union _)

      new HybridGraph(intervals, widths, graphs, vattrs, eattrs)

    } else
      throw new UnsupportedOperationException("directed version of pagerank not yet implemented")
  }

  override def degree(): TemporalGraph[Double, Double] = {
    val allgs = graphs.map(Degree.run(_))

    //now extract values
    val vattrs = allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.map{ case (k,v) => (vid, (k, v.toDouble))}}}.reduce(_ union _)
    val eattrs = edgeattrs.map{ case (k, attr) => (k, (attr._1, 0.0))}

    new HybridGraph(intervals, widths, graphs, vattrs, eattrs)
  }

  override def connectedComponents(): TemporalGraph[VertexId, ED] = {
    def conc(grp: Graph[BitSet,BitSet]): Graph[Map[TimeIndex,VertexId],BitSet] = {
    if (grp.vertices.isEmpty)
        Graph[Map[TimeIndex,VertexId],BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      else {
        ConnectedComponentsXT.runHybrid(grp, intervals.size)
      }
    }

    val allgs = graphs.map(conc)

    //now extract values
    val vattrs = allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.map{ case (k,v) => (vid, (k, v))}}}.reduce(_ union _)

    new HybridGraph(intervals, widths, graphs, vattrs, edgeattrs)
  }

  override def shortestPaths(landmarks: Seq[VertexId]): TemporalGraph[ShortestPathsXT.SPMap, ED] = {
    throw new UnsupportedOperationException("shortest paths not yet implemented")
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    graphs.filterNot(_.edges.isEmpty).map(_.edges.partitions.size).reduce(_ + _)
  }

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): TemporalGraph[VD, ED] = {
    //persist each graph
    graphs.map(_.persist(newLevel))
    vertexattrs.persist(newLevel)
    edgeattrs.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): TemporalGraph[VD, ED] = {
    graphs.map(_.unpersist(blocking))
    vertexattrs.unpersist(blocking)
    edgeattrs.unpersist(blocking)
    this
  }
  
  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int): TemporalGraph[VD, ED] = {
    partitionBy(pst, runs, 0)
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): TemporalGraph[VD, ED] = {
    if (pst != PartitionStrategyType.None) {
      //TODO: figure out the correct second argument to makeStrategy
      new HybridGraph(intervals, widths, graphs.map { g =>
        val numParts: Int = if (parts > 0) parts else g.edges.partitions.size
        g.partitionBy(PartitionStrategies.makeStrategy(pst, 0, intervals.size, runs), numParts)}, vertexattrs, edgeattrs)
    } else
      this
  }

}

object HybridGraph extends Serializable {
  final def loadData(dataPath: String, start: LocalDate, end: LocalDate): HybridGraph[String, Int] = {
    loadWithPartition(dataPath, start, end, PartitionStrategyType.None, 1)
  }

  final def loadWithPartition(dataPath: String, start: LocalDate, end: LocalDate, strategy: PartitionStrategyType.Value, runWidth: Int): HybridGraph[String, Int] = {
    var minDate: LocalDate = start
    var maxDate: LocalDate = end

    var source: scala.io.Source = null
    var fs: FileSystem = null

    val pt: Path = new Path(dataPath + "/Span.txt")
    val conf: Configuration = new Configuration()    
    if (System.getenv("HADOOP_CONF_DIR") != "") {
      conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"))
    }
    fs = FileSystem.get(conf)
    source = scala.io.Source.fromInputStream(fs.open(pt))

    val lines = source.getLines
    val minin = LocalDate.parse(lines.next)
    val maxin = LocalDate.parse(lines.next)
    val res = Resolution.from(lines.next)

    if (minin.isAfter(start)) 
      minDate = minin
    if (maxin.isBefore(end)) 
      maxDate = maxin
    source.close()

    if (minDate.isAfter(maxDate) || minDate.isEqual(maxDate))
      throw new IllegalArgumentException("invalid date range")

    var intvs: Seq[Interval] = scala.collection.immutable.Seq[Interval]()
    var gps: ParSeq[Graph[BitSet,BitSet]] = ParSeq[Graph[BitSet,BitSet]]()
    var vatts: RDD[(VertexId,(TimeIndex,String))] = ProgramContext.sc.emptyRDD
    var eatts: RDD[((VertexId,VertexId),(TimeIndex,Int))] = ProgramContext.sc.emptyRDD
    var xx: LocalDate = minDate
    while (xx.isBefore(maxDate)) {
      intvs = intvs :+ res.getInterval(xx)
      xx = intvs.last.end
    }
    var runs: Seq[Int] = Seq[Int]()

    xx = minDate
    while (xx.isBefore(maxDate)) {
      //FIXME: make this more flexible based on similarity measure
      val remaining = res.numBetween(xx, maxDate) - 1
      val take = math.min(runWidth-1, remaining)
      var end = res.getInterval(xx, take)
      runs = runs :+ (take+1)

      //load some number of consecutive graphs into one
      val usersnp: RDD[(VertexId,(TimeIndex,String))] = MultifileLoad.readNodes(dataPath, xx, end.start).flatMap{ x =>
        val (filename, line) = x
        val dt = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))
        val parts = line.split(",")
        val index = res.numBetween(minDate, dt)
        if (parts.size > 1 && parts.head != "" && index > -1)
          Some((parts.head.toLong, (index, parts(1).toString)))
        else
          None
      }
      val users = usersnp.partitionBy(new HashPartitioner(usersnp.partitions.size)).persist
      val linksnp: RDD[((VertexId,VertexId),(TimeIndex,Int))] = MultifileLoad.readEdges(dataPath, xx, end.start).flatMap{ x =>
        val (filename, line) = x
        val dt = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          var attr = 0
          if(lineArray.length > 2){
            attr = lineArray{2}.toInt
          }
          val index = res.numBetween(minDate, dt)
          if (srcId > dstId)
            Some(((dstId, srcId), (index,attr)))
          else
            Some(((srcId, dstId), (index,attr)))
        } else None
      }
      val links = linksnp.partitionBy(new HashPartitioner(linksnp.partitions.size)).persist
      //TODO: make these hard-coded parameters be dependent on  data size
      //and evolution rate
      val reductFact: Int = math.max(1, (take+1)/2)
      val verts: RDD[(VertexId, BitSet)] = users.mapValues{ v => BitSet(v._1)}.reduceByKey((a,b) => a union b, math.max(4,users.partitions.size/reductFact) )
      val edges = EdgeRDD.fromEdges[BitSet, BitSet](links.mapValues{ v => BitSet(v._1)}.reduceByKey((a,b) => a union b, math.max(4,links.partitions.size/reductFact)).map{case (k,v) => Edge(k._1, k._2, v)})
      var graph: Graph[BitSet,BitSet] = Graph(verts, edges, BitSet())

      if (strategy != PartitionStrategyType.None) {
        graph = graph.partitionBy(PartitionStrategies.makeStrategy(strategy, 0, intvs.size, runWidth))
      }

      gps = gps :+ graph.persist()
      vatts = vatts union users
      eatts = eatts union links
      xx = end.end

      /*
       val degs: RDD[Double] = graph.degrees.map{ case (vid,attr) => attr}
       println("min degree: " + degs.min)
       println("max degree: " + degs.max)
       println("average degree: " + degs.mean)
       val counts = degs.histogram(Array(0.0, 10, 50, 100, 1000, 5000, 10000, 50000, 100000, 250000))
       println("histogram:" + counts.mkString(","))
       println("number of vertices: " + graph.vertices.count)
       println("number of edges: " + graph.edges.count)
       println("number of partitions in edges: " + graph.edges.partitions.size)
      */

    }

    //new HybridGraph(intvs, runs, gps, vatts.partitionBy(new HashPartitioner(vatts.partitions.size / 2)).persist, eatts.partitionBy(new HashPartitioner(eatts.partitions.size / 2)).persist)
    new HybridGraph(intvs, runs, gps, vatts, eatts)
  }

  def emptyGraph[VD: ClassTag, ED: ClassTag](): HybridGraph[VD, ED] = new HybridGraph(Seq[Interval](), Seq[Int](0), ParSeq[Graph[BitSet,BitSet]](), ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
}

object Degree {
  def run(graph: Graph[BitSet,BitSet]): Graph[Map[TimeIndex,Int],BitSet] = {
    def mergeFunc(a:Map[TimeIndex,Int], b:Map[TimeIndex,Int]): Map[TimeIndex,Int] = {
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    val degRDD = graph.aggregateMessages[Map[TimeIndex, Int]](
      ctx => {
        ctx.sendToSrc(ctx.attr.seq.map(x => (x,1)).toMap)
        ctx.sendToDst(ctx.attr.seq.map(x => (x,1)).toMap)
      },
      mergeFunc, TripletFields.None)
    graph.outerJoinVertices(degRDD) {
      case (vid, vdata, Some(deg)) => deg ++ vdata.filter(x => !deg.contains(x)).seq.map(x => (x,0)).toMap
      case (vid, vdata, None) => vdata.seq.map(x => (x,0)).toMap
    }
  }
}
