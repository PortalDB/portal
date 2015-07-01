//For one graph per year (spanshots):
package edu.drexel.cs.dbgroup.graphxt

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import scala.collection.parallel.ParSeq
import scala.collection.mutable.Buffer
import scala.reflect.ClassTag
import scala.util.control._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import org.apache.spark.SparkContext
import org.apache.spark.Partition

import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphLoaderAddon
import org.apache.spark.rdd._
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import edu.drexel.cs.dbgroup.graphxt.util.NumberRangeRegex;
import edu.drexel.cs.dbgroup.graphxt.util.MultifileLoad

class SnapshotGraphParallel[VD: ClassTag, ED: ClassTag](sp: Interval, res: Int, invs: SortedMap[Interval, TimeIndex], gps: ParSeq[Graph[VD, ED]]) extends TemporalGraph[VD, ED] with Serializable {
  val span = sp
  val resolution = res
  val graphs: ParSeq[Graph[VD, ED]] = gps
  val intervals: SortedMap[Interval, TimeIndex] = invs

  /** Default constructor is provided to support serialization */
  protected def this() = this(null, 0, null, null)

  //constructor overloading
  def this(sp: Interval) = {
    this(sp, 1, TreeMap[Interval, Int](), ParSeq[Graph[VD, ED]]())
  }

  override def size(): Int = { graphs.size }

  override def numEdges(): Long = {
    graphs.filterNot(_.edges.isEmpty).map(_.numEdges).reduce(_ + _)
  }

  override def numPartitions(): Int = {
    graphs.filterNot(_.edges.isEmpty).map(_.edges.partitions.size).reduce(_ + _)
  }

  override def vertices: VertexRDD[Map[TimeIndex, VD]] = {
    VertexRDD(graphs.zipWithIndex.filterNot(x => x._1.vertices.isEmpty).map(x => x._1.vertices.mapValues(y => Map[TimeIndex, VD](x._2 -> y)))
      .reduce((a, b) => VertexRDD(a union b))
      .reduceByKey((a: Map[TimeIndex, VD], b: Map[TimeIndex, VD]) => a ++ b))
  }

  override def verticesFlat: VertexRDD[(TimeIndex, VD)] = {
    VertexRDD(graphs.zipWithIndex.filterNot(x => x._1.vertices.isEmpty)
      .map(x => x._1.vertices.mapValues(y => (x._2, y)))
      .reduce((a, b) => VertexRDD(a union b)))
  }

  override def edges: EdgeRDD[Map[TimeIndex, ED]] = {
    EdgeRDD.fromEdges[Map[TimeIndex, ED], VD](graphs.zipWithIndex.filterNot(x => x._1.edges.isEmpty).map(x => x._1.edges.mapValues(y => Map[TimeIndex, ED](x._2 -> y.attr)))
      .reduce((a: RDD[Edge[Map[TimeIndex, ED]]], b: RDD[Edge[Map[TimeIndex, ED]]]) => a union b)
      .map(x => ((x.srcId, x.dstId), x.attr))
      .reduceByKey((a: Map[TimeIndex, ED], b: Map[TimeIndex, ED]) => a ++ b)
      .map(x => Edge(x._1._1, x._1._2, x._2))
    )
  }

  override def edgesFlat: EdgeRDD[(TimeIndex, ED)] = {
    EdgeRDD.fromEdges[(TimeIndex, ED), VD](graphs.zipWithIndex.filterNot(x => x._1.edges.isEmpty).map(x => x._1.edges.mapValues(y => (x._2,y.attr)))
      .reduce((a: RDD[Edge[(TimeIndex, ED)]], b: RDD[Edge[(TimeIndex, ED)]]) => a union b))
  }

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): SnapshotGraphParallel[VD, ED] = {
    //persist each graph
    graphs.map(_.persist(newLevel))
    this
  }

  override def unpersist(blocking: Boolean = true): SnapshotGraphParallel[VD, ED] = {
    graphs.map(_.unpersist(blocking))
    this
  }

  //intervals are assumed to be nonoverlapping
  //Note: snapshots should be added from earliest to latest for performance reasons
  //FIXME: this assumes that the span remains unchanged
  override def addSnapshot(place: Interval, snap: Graph[VD, ED]): SnapshotGraphParallel[VD, ED] = {
    var intvs = intervals
    var gps = graphs
    val iter: Iterator[Interval] = intvs.keysIterator
    var pos: Int = -1
    var found: Boolean = false

    // create a Breaks object (to break out of a loop)
    val loop = new Breaks

    loop.breakable {
      //this is not as efficient as binary search but the list is expected to be short
      while (iter.hasNext) {
        val nx: Interval = iter.next

        if (nx > place) {
          pos = intvs(nx) - 1
          found = true
          loop.break
        }
      }
    }

    //pos can be negative if there are no elements
    //or if this interval is the smallest of all
    //or if this interval is the largest of all
    if (pos < 0) {
      if (found) {
        //put in position 0, move the rest to the right
        pos = 0
        intvs.map { case (k, v) => (k, v + 1) }
        intvs += (place -> pos)
        gps = snap +: gps
      } else {
        //put in last
        intvs += (place -> gps.size)
        gps = gps :+ snap
      }
    } else {
      //put in the specified position, move the rest to the right
      intvs.foreach { case (k, v) => if (v > pos) (k, v + 1) }
      val (st, en) = gps.splitAt(pos - 1)
      gps = (st ++ (snap +: en))
    }

    new SnapshotGraphParallel(span, resolution, intvs, gps)
  }

  override def getSnapshotByTime(time: TimeIndex): Graph[VD, ED] = {
    if (time >= span.min && time <= span.max) {
      //retrieve the position of the correct interval
      var position = -1
      val iter: Iterator[Interval] = intervals.keysIterator
      val loop = new Breaks

      loop.breakable {
        while (iter.hasNext) {
          val nx: Interval = iter.next
          if (nx.contains(time)) {
            position = intervals(nx)
            loop.break
          }
        }
      }

      getSnapshotByPosition(position)
    } else
      null
  }

  override def getSnapshotByPosition(pos: TimeIndex): Graph[VD, ED] = {
    if (pos >= 0 && pos < graphs.size)
      graphs(pos)
    else
      null
  }

  //since SnapshotGraphParallels are involatile (except with add)
  //the result is a new SnapshotGraphParallel
  override def select(bound: Interval): SnapshotGraphParallel[VD, ED] = {
    if (span.min == bound.min && span.max == bound.max) return this

    if (!span.intersects(bound)) {
      return null
    }

    val minBound = if (bound.min > span.min) bound.min else span.min
    val maxBound = if (bound.max < span.max) bound.max else span.max
    val rng = Interval(minBound, maxBound)

    var selectStart = math.abs(span.min - minBound)
    var selectStop = maxBound - span.min + 1
    var intervalFilter: SortedMap[Interval, Int] = intervals.slice(selectStart, selectStop)

    val newGraphs: ParSeq[Graph[VD, ED]] = intervalFilter.map(x => graphs(x._2)).toSeq.par
    val newIntervals: SortedMap[Interval, Int] = intervalFilter.mapValues { x => x - selectStart }

    new SnapshotGraphParallel(rng, resolution, newIntervals, newGraphs)
  }

  override def aggregate(resolution: Int, sem: AggregateSemantics.Value, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): SnapshotGraphParallel[VD, ED] = {
    var intvs: SortedMap[Interval, TimeIndex] = TreeMap[Interval, TimeIndex]()
    var gps: ParSeq[Graph[VD, ED]] = ParSeq[Graph[VD, ED]]()

    val iter: Iterator[(Interval, Int)] = intervals.iterator
    var numResults: Int = 0
    var minBound, maxBound: Int = 0;

    while (iter.hasNext) {
      val (k, v) = iter.next
      minBound = k.min
      maxBound = k.max

      //take resolution# of consecutive graphs, combine them according to the semantics
      var firstVRDD: RDD[(VertexId, VD)] = graphs(v).vertices
      var firstERDD: RDD[Edge[ED]] = graphs(v).edges
      var yy = 0
      var width = 0

      val loop = new Breaks
      loop.breakable {
        for (yy <- 1 to resolution - 1) {

          if (iter.hasNext) {
            val (k, v) = iter.next
            firstVRDD = firstVRDD.union(graphs(v).vertices)
            firstERDD = firstERDD.union(graphs(v).edges)
            maxBound = k.max
          } else {
            width = yy - 1
            loop.break
          }
        }
      }
      if (width == 0) width = resolution - 1

      intvs += (Interval(minBound, maxBound) -> numResults)
      numResults += 1

      if (sem == AggregateSemantics.Existential) {
        gps = gps :+ Graph(VertexRDD(firstVRDD.reduceByKey(vAggFunc)), EdgeRDD.fromEdges[ED, VD](firstERDD.map(e => ((e.srcId, e.dstId), e.attr)).reduceByKey(eAggFunc).map { x =>
          val (k, v) = x
          Edge(k._1, k._2, v)
        }))
      } else if (sem == AggregateSemantics.Universal) {
        gps = gps :+ Graph(firstVRDD.map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (vAggFunc(x._1, y._1), x._2 + y._2)).filter(x => x._2._2 > width).map { x =>
          val (k, v) = x
          (k, v._1)
        },
          EdgeRDD.fromEdges[ED, VD](firstERDD.map(e => ((e.srcId, e.dstId), (e.attr, 1))).reduceByKey((x, y) => (eAggFunc(x._1, y._1), x._2 + y._2)).filter(x => x._2._2 > width).map { x =>
            val (k, v) = x
            Edge(k._1, k._2, v._1)
          }))
      }
    }

    new SnapshotGraphParallel(span, resolution * this.resolution, intvs, gps)
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int): SnapshotGraphParallel[VD, ED] = {
    partitionBy(pst, runs, graphs.head.edges.partitions.size)
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): SnapshotGraphParallel[VD, ED] = {
    var numParts = if (parts > 0) parts else graphs.head.edges.partitions.size

    if (pst != PartitionStrategyType.None) {
      //not changing the intervals, only the graphs at their indices
      //each partition strategy for SG needs information about the graph

      //use that strategy to partition each of the snapshots
      var temp = intervals.map { case (a, b) => graphs(b).partitionBy(PartitionStrategies.makeStrategy(pst, b, graphs.size, runs), numParts) }
      var gps: ParSeq[Graph[VD, ED]] = temp.toSeq.par

      new SnapshotGraphParallel(span, resolution, intervals, gps)
    } else
      this
  }

  override def degrees: VertexRDD[Map[TimeIndex, Int]] = {
    VertexRDD(graphs.zipWithIndex.map(x => x._1.degrees.mapValues(deg => Map[TimeIndex, Int](x._2 -> deg)))
      .reduce((x,y) => VertexRDD(x union y))
      .reduceByKey((a: Map[TimeIndex, Int], b: Map[TimeIndex, Int]) => a ++ b))
  }

  override def union(other: TemporalGraph[VD, ED]): TemporalGraph[VD, ED] = {
    var grp2: SnapshotGraphParallel[VD, ED] = other match {
      case grph: SnapshotGraphParallel[VD, ED] => grph
      case _ => throw new ClassCastException
    }
    if (resolution != grp2.resolution) {
      println("The resolutions of the two graphs are not the same, ignoring")
      return this
    }

    //compute the span
    val minBound = if (span.min > grp2.span.min) grp2.span.min else span.min
    val maxBound = if (span.max < grp2.span.max) grp2.span.max else span.max

    //compute the new intervals
    //because we use 0-indexed intervals, we cannot just take a union of two intervals
    //we need to reindex first

    //create intervals for missing graphs
    def makeFullIndex(intvs: SortedMap[Interval, TimeIndex], sp: Interval, res: Int): SortedMap[Interval, Boolean] = {
      var xx = 0
      var result: SortedMap[Interval, Boolean] = TreeMap[Interval, Boolean]()
      val inverted = intvs.map(_.swap)
      var maxx: TimeIndex = inverted.max._1
      //need to find the last index even if some graphs at the end of the span are missing
      if (inverted.max._2.max < sp.max) maxx += (sp.max - inverted.max._2.max)/res
      var btm = sp.min
      for (xx <- 0 to maxx) {
        if (inverted.contains(xx)) {
          result += (inverted(xx) -> true)
          btm = inverted(xx).max + 1
        } else {
          result += (Interval(btm, btm+res-1) -> false)
          btm = btm + res
        }
      }
      result
    }

    //merge intervals, both missing and present, from both graphs
    //filter out the falses, then map back into map of intervals to indices
    val mergedIntervals: SortedMap[Interval, TimeIndex] = (makeFullIndex(intervals, span, resolution) ++ makeFullIndex(grp2.intervals, grp2.span, grp2.resolution)).zipWithIndex.filter(x => x._1._2 == true).map(x => (x._1._1,x._2))

    //now the graphs themselves, which are arranged in a sequence
    //look up the new index of the earliest graph
    def makeNewIndexGraph(grh: SnapshotGraphParallel[VD, ED]): ParSeq[(Graph[VD, ED], Int)] = {
      val newmin = mergedIntervals(grh.intervals.head._1)
      grh.graphs.zipWithIndex.map(x => (x._1, x._2 + newmin))
    }

    val graphBuf: Buffer[Graph[VD, ED]] = Buffer[Graph[VD, ED]]()
    makeNewIndexGraph(this).foreach(x => graphBuf(x._2) = x._1)
    //FIXME: what if there is a graph for the same index?
    makeNewIndexGraph(grp2).foreach(x => graphBuf(x._2) = x._1)

    new SnapshotGraphParallel(Interval(minBound, maxBound), resolution, mergedIntervals, graphBuf.par)
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, Map[TimeIndex, U])])(mapFunc: (VertexId, TimeIndex, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val newseq: ParSeq[Graph[VD2, ED]] = graphs.zipWithIndex.map(x => 
      x._1.outerJoinVertices(other){ (id: VertexId, attr1: VD, attr2: Option[Map[TimeIndex, U]]) =>
        //this function receives a (id, attr, Map)
        //need to instead call mapFunc with (id, index, attr, otherattr)
        if (attr2.isEmpty)
          mapFunc(id, x._2, attr1, None)
        else
          mapFunc(id, x._2, attr1, attr2.get.get(x._2)) })
    new SnapshotGraphParallel(span, resolution, intervals, newseq)
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, TimeIndex, VD) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val newseq: ParSeq[Graph[VD2, ED]] = graphs.zipWithIndex.map(x =>
      x._1.mapVertices{ (id: VertexId, attr: VD) => map(id, x._2, attr) })
    new SnapshotGraphParallel(span, resolution, intervals, newseq)
  }

  override def mapEdges[ED2: ClassTag](map: (Edge[ED], TimeIndex) => ED2): TemporalGraph[VD, ED2] = {
    val newseq: ParSeq[Graph[VD, ED2]] = graphs.zipWithIndex.map(x =>
      x._1.mapEdges{ e: Edge[ED] => map(e, x._2)})
    new SnapshotGraphParallel(span, resolution, intervals, newseq)
  }

  override def pregel[A: ClassTag]
     (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
       sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): TemporalGraph[VD, ED] = {
    val newseq: ParSeq[Graph[VD, ED]] = graphs.map(x => Pregel(x, initialMsg,
      maxIterations, activeDirection)(vprog, sendMsg, mergeMsg))
    new SnapshotGraphParallel(span, resolution, intervals, newseq)
  }

  //run PageRank on each contained snapshot
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): SnapshotGraphParallel[Double, Double] = {
      def safePagerank(grp: Graph[VD, ED]): Graph[Double, Double] = {
        if (grp.edges.isEmpty) {
          Graph[Double, Double](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
        } else {
          if (uni)
            UndirectedPageRank.run(Graph(grp.vertices, grp.edges.coalesce(1, true)), tol, resetProb, numIter)
          else
            //FIXME: this doesn't use the numIterations stop condition
            grp.pageRank(tol, resetProb)
        }
      }

    val newseq: ParSeq[Graph[Double, Double]] = graphs.map(safePagerank)
    new SnapshotGraphParallel(span, resolution, intervals, newseq)
  }

  override def connectedComponents(): SnapshotGraphParallel[VertexId, ED] = {
    var intvs: SortedMap[Interval, Int] = TreeMap[Interval, Int]()
    var gps: ParSeq[Graph[Long, ED]] = ParSeq[Graph[Long, ED]]()
    val iter: Iterator[(Interval, Int)] = intervals.iterator
    var numResults: Int = 0

    while (iter.hasNext) {
      val (k, v) = iter.next
      intvs += (k -> numResults)
      numResults += 1

      if (graphs(v).vertices.isEmpty) {
        gps = gps :+ Graph[Long, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        var graph = graphs(v).connectedComponents()
        gps = gps :+ graph
      }
    }

    new SnapshotGraphParallel(span, resolution, intvs, gps)
  }

  override def shortestPaths(landmarks: Seq[VertexId]): SnapshotGraphParallel[ShortestPathsXT.SPMap, ED] = {
    var intvs: SortedMap[Interval, Int] = TreeMap[Interval, Int]()
    var gps: ParSeq[Graph[ShortestPathsXT.SPMap, ED]] = ParSeq[Graph[ShortestPathsXT.SPMap, ED]]()
    val iter: Iterator[(Interval, Int)] = intervals.iterator
    var numResults: Int = 0

    while (iter.hasNext) {
      val (k, v) = iter.next
      intvs += (k -> numResults)
      numResults += 1

      if (graphs(v).vertices.isEmpty) {
        gps = gps :+ Graph[ShortestPathsXT.SPMap, ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        var graph = ShortestPathsXT.run(graphs(v), landmarks)
        gps = gps :+ graph
      }
    }

    new SnapshotGraphParallel(span, resolution, intvs, gps)
  }

}

object SnapshotGraphParallel extends Serializable {
  //this assumes that the data is in the dataPath directory, each time period in its own files
  //and the naming convention is nodes<time>.txt and edges<time>.txt
  //TODO: extend to be more flexible about input data such as arbitrary uniform intervals are supported
  final def loadData(dataPath: String, startIndex: TimeIndex = 0, endIndex: TimeIndex = Int.MaxValue): SnapshotGraphParallel[String, Int] = {
    var minYear: Int = Int.MaxValue
    var maxYear: Int = 0

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
    val minin = lines.next.toInt
    val maxin = lines.next.toInt
    minYear = if (minin > startIndex) minin else startIndex
    maxYear = if (maxin < endIndex) maxin else endIndex
    source.close()

    val span = Interval(minYear, maxYear)
    var years = 0
    var intvs: SortedMap[Interval, Int] = TreeMap[Interval, Int]()
    var gps: ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]]()

    for (years <- minYear to maxYear) {
      var nodesPath = dataPath + "/nodes/nodes" + years + ".txt"
      var edgesPath = dataPath + "/edges/edges" + years + ".txt"
      var numNodeParts = MultifileLoad.estimateParts(nodesPath) 
      var numEdgeParts = MultifileLoad.estimateParts(edgesPath) 
      
      val users: RDD[(VertexId, String)] = ProgramContext.sc.textFile(dataPath + "/nodes/nodes" + years + ".txt", numNodeParts).map(line => line.split(",")).map { parts =>
        if (parts.size > 1 && parts.head != "")
          (parts.head.toLong, parts(1).toString)
        else
          (0L, "Default")
      }
      
      var edges: EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.emptyRDD)

      val ept: Path = new Path(dataPath + "/edges/edges" + years + ".txt")
      if (fs.exists(ept) && fs.getFileStatus(ept).getLen > 0) {
        //uses extended version of Graph Loader to load edges with attributes
        val tmp = years
        edges = GraphLoaderAddon.edgeListFile(ProgramContext.sc, dataPath + "/edges/edges" + years + ".txt", true, numEdgeParts).edges
      }
      
      val graph: Graph[String, Int] = Graph(users, edges)

      intvs += (Interval(years, years) -> (years - minYear))
      gps = gps :+ graph
    }
    
    new SnapshotGraphParallel(span, 1, intvs, gps)
  }
}
