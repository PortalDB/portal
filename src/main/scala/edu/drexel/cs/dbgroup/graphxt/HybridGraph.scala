package edu.drexel.cs.dbgroup.graphxt

import scala.collection.parallel.ParSeq
import scala.collection.immutable.BitSet
import scala.collection.breakOut

import scala.reflect.ClassTag
import scala.util.control._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import org.apache.spark.graphx._
import org.apache.spark.rdd._

import edu.drexel.cs.dbgroup.graphxt.util.MultifileLoad

import java.time.LocalDate

class HybridGraph[VD: ClassTag, ED: ClassTag](intvs: Seq[Interval], gps: ParSeq[Graph[BitSet, BitSet]], veratts: RDD[((VertexId,TimeIndex),VD)], edgatts: RDD[((VertexId,VertexId,TimeIndex),ED)]) extends TemporalGraph[VD, ED] with Serializable {
  val graphs: ParSeq[Graph[BitSet, BitSet]] = gps
  val resolution: Resolution = if (intvs.size > 0) intvs.head.resolution else Resolution.zero

  intvs.foreach { ii =>
    if (!ii.resolution.isEqual(resolution))
      throw new IllegalArgumentException("temporal sequence is not valid, intervals are not all of equal resolution")
  }

  val intervals: Seq[Interval] = intvs
  lazy val span: Interval = if (intervals.size > 0) Interval(intervals.head.start, intervals.last.end) else Interval(LocalDate.now, LocalDate.now)

  //vertex attributes are kept in a separate rdd with an id,time key
  val vertexattrs: RDD[((VertexId,TimeIndex),VD)] = veratts

  //edge attributes are kept in a separate rdd with an id,id,time key
  val edgeattrs: RDD[((VertexId,VertexId,TimeIndex),ED)] = edgatts

  /** Default constructor is provided to support serialization */
  protected def this() = this(Seq[Interval](), ParSeq[Graph[BitSet,BitSet]](), ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)

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

  override def vertices: VertexRDD[Map[Interval, VD]] = {
    val start = span.start
    VertexRDD(vertexattrs.map{ case (k,v) => (k._1, Map[Interval,VD](resolution.getInterval(start, k._2) -> v))}
    .reduceByKey((a: Map[Interval, VD], b: Map[Interval, VD]) => a ++ b))
  }

  override def verticesFlat: VertexRDD[(Interval, VD)] = {
    val start = span.start
    VertexRDD(vertexattrs.map{ case (k,v) => (k._1, (resolution.getInterval(start, k._2), v))})
  }

  override def edges: EdgeRDD[Map[Interval, ED]] = {
    val start = span.start
    EdgeRDD.fromEdges[Map[Interval,ED], VD](edgeattrs.map{ case (k,v) => ((k._1, k._2), Map[Interval,ED](resolution.getInterval(start, k._3) -> v))}
      .reduceByKey((a: Map[Interval, ED], b: Map[Interval, ED]) => a ++ b)
      .map(x => Edge(x._1._1, x._1._2, x._2)))
  }

  override def edgesFlat: EdgeRDD[(Interval, ED)] = {
    val start = span.start
    EdgeRDD.fromEdges[(Interval, ED), VD](edgeattrs.map{ case (k,v) => Edge(k._1, k._2, (resolution.getInterval(start, k._3), v))})
  }

  override def degrees: VertexRDD[Map[Interval, Int]] = {
    throw new UnsupportedOperationException("degrees not yet implemented")
  }

  override def getTemporalSequence: Seq[Interval] = intervals

  override def getSnapshot(period: Interval): Graph[VD, ED] = {
    throw new UnsupportedOperationException("getSnapshot not yet implemented")
  }

  /** Query operations */
  
  override def select(bound: Interval): TemporalGraph[VD, ED] = {
    throw new UnsupportedOperationException("select not yet implemented")
  }

  override def select(tpred: Interval => Boolean): TemporalGraph[VD, ED] = {
    throw new UnsupportedOperationException("select not yet implemented")
  }

  override def select(epred: EdgeTriplet[VD, ED] => Boolean, vpred: (VertexId, VD) => Boolean): TemporalGraph[VD, ED] = {
    throw new UnsupportedOperationException("select not yet impelemented")
  }

  override def aggregate(res: Resolution, sem: AggregateSemantics.Value, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TemporalGraph[VD, ED] = {
    throw new UnsupportedOperationException("aggregate not yet implemented")
  }

  override def transform[ED2: ClassTag, VD2: ClassTag](emap: (Edge[ED], Interval) => ED2, vmap: (VertexId, Interval, VD) => VD2): TemporalGraph[VD2, ED2] = {
    val start = span.start
    new HybridGraph[VD2, ED2](intervals, graphs, vertexattrs.map{ case (k,v) => (k, vmap(k._1, resolution.getInterval(start, k._2), v))}, edgeattrs.map{ case (k,v) => (k, emap(Edge(k._1, k._2, v), resolution.getInterval(start, k._3)))})
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val start = span.start
    new HybridGraph[VD2, ED](intervals, graphs, vertexattrs.map{ case (k,v) => (k, map(k._1, resolution.getInterval(start, k._2), v))}, edgeattrs)
  }

  override def mapEdges[ED2: ClassTag](map: (Edge[ED], Interval) => ED2): TemporalGraph[VD, ED2] = {
    val start = span.start
    new HybridGraph[VD, ED2](intervals, graphs, vertexattrs, edgeattrs.map{ case (k,v) => (k, map(Edge(k._1, k._2, v), resolution.getInterval(start, k._3)))})
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, Map[Interval, U])])(mapFunc: (VertexId, Interval, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val start = span.start

    val in: RDD[((VertexId,TimeIndex),U)] = other.flatMap(x => x._2.map(y => ((x._1, resolution.numBetween(start, y._1.start)), y._2)))

    new HybridGraph[VD2, ED](intervals, graphs, vertexattrs.leftOuterJoin(in).map{ case (k,v) => (k, mapFunc(k._1, resolution.getInterval(start, k._2), v._1, v._2))}, edgeattrs)
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

    def mergeFunc(a:Map[TimeIndex,Int], b:Map[TimeIndex,Int]): Map[TimeIndex,Int] = {
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    def vertexProgram(id: VertexId, attr: Map[TimeIndex, (Double,Double)], msg: Map[TimeIndex, Double]): Map[TimeIndex, (Double,Double)] = {
      var vals = attr
      msg.foreach { x =>
        val (k,v) = x
        if (vals.contains(k)) {
          val (oldPR, lastDelta) = vals(k)
          val newPR = oldPR + (1.0 - resetProb) * msg(k)
          vals = vals.updated(k,(newPR,newPR-oldPR))
        }
      }
      vals
    }

    def sendMessage(edge: EdgeTriplet[Map[TimeIndex,(Double,Double)], Map[TimeIndex, (Double,Double)]]) = {
      //need to generate an iterator of messages for each index
      edge.attr.iterator.flatMap{ case (k,v) =>
        if (edge.srcAttr(k)._2 > tol &&
          edge.dstAttr(k)._2 > tol) {
          Iterator((edge.dstId, Map((k -> edge.srcAttr(k)._2 * v._1))), (edge.srcId, Map((k -> edge.dstAttr(k)._2 * v._2))))
        } else if (edge.srcAttr(k)._2 > tol) {
          Iterator((edge.dstId, Map((k -> edge.srcAttr(k)._2 * v._1))))
        } else if (edge.dstAttr(k)._2 > tol) {
          Iterator((edge.srcId, Map((k -> edge.dstAttr(k)._2 * v._2))))
        } else {
          Iterator.empty
        }
      }
        .toSeq.groupBy{ case (k,v) => k}
      //we now have a Map[VertexId, Seq[(VertexId, Map[TimeIndex,Double])]]
        .mapValues(v => v.map{case (k,m) => m}.reduce((a,b) => a ++ b))
        .iterator
    }

    def messageCombiner(a: Map[TimeIndex,Double], b: Map[TimeIndex,Double]): Map[TimeIndex,Double] = {
      (a.keySet ++ b.keySet).map { i =>
        val count1Val:Double = a.getOrElse(i, 0.0)
        val count2Val:Double = b.getOrElse(i, 0.0)
        i -> (count1Val + count2Val)
      }.toMap
    }

    if (uni) {
      def prank(grp: Graph[BitSet,BitSet]): Graph[Map[TimeIndex,(Double,Double)], Map[TimeIndex,(Double,Double)]] = {
        if (grp.edges.isEmpty)
          Graph[Map[TimeIndex,(Double,Double)],Map[TimeIndex,(Double,Double)]](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
        else {
          val degs: VertexRDD[Map[TimeIndex, Int]] = grp.aggregateMessages[Map[TimeIndex, Int]](
            ctx => {
              ctx.sendToSrc(ctx.attr.seq.map(x => (x,1)).toMap)
              ctx.sendToDst(ctx.attr.seq.map(x => (x,1)).toMap)
            },
            mergeFunc, TripletFields.None)
          val prankGraph: Graph[Map[TimeIndex, (Double,Double)], Map[TimeIndex, (Double,Double)]] = grp.outerJoinVertices(degs) {
            case (vid, vdata, Some(deg)) => deg ++ vdata.filter(x => !deg.contains(x)).seq.map(x => (x,0)).toMap
            case (vid, vdata, None) => vdata.seq.map(x => (x,0)).toMap
          }
            .mapTriplets( e =>  e.attr.seq.map(x => (x, (1.0 / e.srcAttr(x), 1.0 / e.dstAttr(x)))).toMap)
            .mapVertices( (id,attr) => attr.mapValues{ x => (0.0,0.0)}.map(identity))
            .cache()

          val initialMessage: Map[TimeIndex,Double] = (for(i <- 0 to intervals.size) yield (i -> resetProb / (1.0 - resetProb)))(breakOut)

          Pregel(prankGraph, initialMessage, numIter, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)
        }
      }
    
      var allgs:ParSeq[Graph[Map[TimeIndex,(Double,Double)], Map[TimeIndex,(Double,Double)]]] = graphs.map(prank)

      //now extract values
      val vattrs= allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.map{ case (k,v) => ((vid, k), v._1)}}}.reduce(_ union _)
      val eattrs = allgs.map{ g => g.edges.flatMap{ e => e.attr.map{ case (k,v) => ((e.srcId, e.dstId, k), v._1)}}}.reduce(_ union _)

      new HybridGraph(intervals, graphs, vattrs, eattrs)

    } else
      throw new UnsupportedOperationException("directed version of pagerank not yet implemented")
  }

  override def degree(): TemporalGraph[Double, Double] = {
    def mergeFunc(a:Map[TimeIndex,Int], b:Map[TimeIndex,Int]): Map[TimeIndex,Int] = {
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }
    
    def deg(grp: Graph[BitSet,BitSet]): Graph[Map[TimeIndex,Int],BitSet] = {
      val degRDD = grp.aggregateMessages[Map[TimeIndex, Int]](
        ctx => {
          ctx.sendToSrc(ctx.attr.seq.map(x => (x,1)).toMap)
          ctx.sendToDst(ctx.attr.seq.map(x => (x,1)).toMap)
        },
        mergeFunc, TripletFields.None)
      grp.outerJoinVertices(degRDD) {
        case (vid, vdata, Some(deg)) => deg ++ vdata.filter(x => !deg.contains(x)).seq.map(x => (x,0)).toMap
        case (vid, vdata, None) => vdata.seq.map(x => (x,0)).toMap
      }
    }

    val allgs = graphs.map(deg)

    //now extract values
    val vattrs = allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.map{ case (k,v) => ((vid, k), v.toDouble)}}}.reduce(_ union _)
    val eattrs = edgeattrs.map{ case (k, attr) => (k, 0.0)}

    new HybridGraph(intervals, graphs, vattrs, eattrs)
  }

  override def connectedComponents(): TemporalGraph[VertexId, ED] = {
    def conc(grp: Graph[BitSet,BitSet]): Graph[Map[TimeIndex,VertexId],BitSet] = {
      if (grp.vertices.isEmpty)
        Graph[Map[TimeIndex,VertexId],BitSet](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      else {
        val conGraph: Graph[Map[TimeIndex, VertexId], BitSet] = grp.mapVertices{ case (vid, bset) => bset.map(x => (x,vid)).toMap}
        def vertexProgram(id: VertexId, attr: Map[TimeIndex, VertexId], msg: Map[TimeIndex, VertexId]): Map[TimeIndex, VertexId] = {
          var vals = attr
          msg.foreach { x =>
            val (k,v) = x
            if (vals.contains(k)) {
              vals = vals.updated(k, math.min(v, vals(k)))
            }
          }
          vals
        }
        def sendMessage(edge: EdgeTriplet[Map[TimeIndex, VertexId], BitSet]): Iterator[(VertexId, Map[TimeIndex, VertexId])] = {
          edge.attr.iterator.flatMap{ k =>
            if (edge.srcAttr(k) < edge.dstAttr(k))
              Iterator((edge.dstId, Map(k -> edge.srcAttr(k))))
            else if (edge.srcAttr(k) > edge.dstAttr(k))
              Iterator((edge.srcId, Map(k -> edge.dstAttr(k))))
            else
              Iterator.empty
          }
            .toSeq.groupBy{ case (k,v) => k}
            .mapValues(v => v.map{ case (k,m) => m}.reduce((a,b) => a ++ b))
            .iterator
        }
        def messageCombiner(a: Map[TimeIndex, VertexId], b: Map[TimeIndex, VertexId]): Map[TimeIndex, VertexId] = {
          (a.keySet ++ b.keySet).map { i =>
            val val1: VertexId = a.getOrElse(i, Long.MaxValue)
            val val2: VertexId = b.getOrElse(i, Long.MaxValue)
            i -> math.min(val1, val2)
          }.toMap
        }

        val i: Int = 0
        val initialMessage: Map[TimeIndex, VertexId] = (for(i <- 0 to intervals.size) yield (i -> Long.MaxValue))(breakOut)

        Pregel(conGraph, initialMessage, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)
      }
    }

    val allgs = graphs.map(conc)

    //now extract values
    val vattrs = allgs.map{ g => g.vertices.flatMap{ case (vid,vattr) => vattr.map{ case (k,v) => ((vid, k), v)}}}.reduce(_ union _)

    new HybridGraph(intervals, graphs, vattrs, edgeattrs)
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
      new HybridGraph(intervals, graphs.map { g => 
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

    var intvs: Seq[Interval] = Seq[Interval]()
    var gps: ParSeq[Graph[BitSet,BitSet]] = ParSeq[Graph[BitSet,BitSet]]()
    var vatts: RDD[((VertexId,TimeIndex),String)] = ProgramContext.sc.emptyRDD
    var eatts: RDD[((VertexId,VertexId,TimeIndex),Int)] = ProgramContext.sc.emptyRDD
    var xx: LocalDate = minDate
    while (xx.isBefore(maxDate)) {
      intvs = intvs :+ res.getInterval(xx)
      xx = intvs.last.end
    }

    xx = minDate
    while (xx.isBefore(maxDate)) {
      //FIXME: make this more flexible based on similarity measure
      val end = res.getInterval(xx, 7)

      //load some number of consecutive graphs into one
      val users: RDD[((VertexId,TimeIndex),String)] = MultifileLoad.readNodes(dataPath, xx, end.start).flatMap{ x =>
        val (filename, line) = x
        val dt = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))
        val parts = line.split(",")
        val index = res.numBetween(intvs.last.start, dt)
        if (parts.size > 1 && parts.head != "" && index > -1)
          Some((parts.head.toLong, index), parts(1).toString)
        else
          None
      }
      val links: RDD[((VertexId,VertexId,TimeIndex),Int)] = MultifileLoad.readEdges(dataPath, xx, end.start).flatMap{ x =>
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
          val index = res.numBetween(intvs.last.start, dt)
          if (srcId > dstId)
            Some((dstId, srcId, index),attr)
          else
            Some((srcId, dstId, index),attr)
        } else None
      }
      val verts: RDD[(VertexId, BitSet)] = users.map{ case (k,v) => (k._1, BitSet(k._2))}.reduceByKey((a,b) => a union b )
      val edges = EdgeRDD.fromEdges[BitSet, BitSet](links.map{ case (k,v) => ((k._1, k._2), BitSet(k._3))}.reduceByKey((a,b) => a union b).map{case (k,v) => Edge(k._1, k._2, v)})
      var graph: Graph[BitSet,BitSet] = Graph(verts, edges, BitSet())

      if (strategy != PartitionStrategyType.None)
        graph = graph.partitionBy(PartitionStrategies.makeStrategy(strategy, 0, intvs.size, runWidth))

      gps = gps :+ graph
      vatts = vatts union users
      eatts = eatts union links
      xx = end.end
    }

    new HybridGraph(intvs, gps, vatts, eatts)
  }
}
