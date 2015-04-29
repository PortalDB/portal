//For one graph per year (spanshots):
package edu.drexel.cs.dbgroup.graphxt

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
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

class SnapshotGraph[VD: ClassTag, ED: ClassTag](sp: Interval, intvs: SortedMap[Interval, Int], grs: Seq[Graph[VD, ED]]) extends TemporalGraph[VD, ED] {
  var span = sp
  var graphs: Seq[Graph[VD, ED]] = grs
  var intervals: SortedMap[Interval, Int] = intvs

  def this(sp: Interval) = {
    this(sp, TreeMap[Interval, Int](), Seq[Graph[VD, ED]]())
  }

  def size(): Int = { graphs.size }

  def numEdges(): Long = {
    val iter: Iterator[Graph[VD, ED]] = graphs.iterator
    var sum: Long = 0L
    while (iter.hasNext) {
      val g = iter.next
      if (!g.edges.isEmpty) {
        sum += g.numEdges
      }
    }
    sum
  }

  def numPartitions(): Int = {
    val iter: Iterator[Graph[VD, ED]] = graphs.iterator
    var allps: Array[Partition] = Array[Partition]()
    while (iter.hasNext) {
      val g = iter.next
      if (!g.edges.isEmpty) {
        allps = allps union g.edges.partitions
      }
    }
    allps.size
  }

  def vertices: VertexRDD[VD] = {
    val iter: Iterator[Graph[VD, ED]] = graphs.iterator
    var result: RDD[(VertexId, VD)] = ProgramContext.sc.emptyRDD
    while (iter.hasNext) {
      val g = iter.next
      if (!g.vertices.isEmpty)
        result = result union g.vertices
    }

    VertexRDD(result) //FIXME: should call .distinct ??
  }

  def edges: EdgeRDD[ED] = {
    val iter: Iterator[Graph[VD, ED]] = graphs.iterator
    var result: RDD[Edge[ED]] = ProgramContext.sc.emptyRDD
    while (iter.hasNext) {
      val g = iter.next
      if (!g.edges.isEmpty)
        result = result union g.edges
    }

    EdgeRDD.fromEdges[ED, VD](result) //FIXME: should call .distinct ??
  }

  def persist(newLevel: StorageLevel = MEMORY_ONLY): SnapshotGraph[VD, ED] = {
    //persist each graph
    val iter = graphs.iterator
    while (iter.hasNext)
      iter.next.persist(newLevel)
    this
  }

  def unpersist(blocking: Boolean = true): SnapshotGraph[VD, ED] = {
    val iter = graphs.iterator
    while (iter.hasNext)
      iter.next.unpersist(blocking)
    this
  }

  //intervals are assumed to be nonoverlapping
  //Note: snapshots should be added from earliest to latest for performance reasons
  def addSnapshot(place: Interval, snap: Graph[VD, ED]): SnapshotGraph[VD, ED] = {
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

    new SnapshotGraph(span, intvs, gps)
  }

  def getSnapshotByTime(time: Int): Graph[VD, ED] = {
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

  def getSnapshotByPosition(pos: Int): Graph[VD, ED] = {
    if (pos >= 0 && pos < graphs.size)
      graphs(pos)
    else
      null
  }

  //since SnapshotGraphs are involatile (except with add)
  //the result is a new snapshotgraph
  def select(bound: Interval): SnapshotGraph[VD, ED] = {
    if (span.intersects(bound)) {
      val minBound = if (bound.min > span.min) bound.min else span.min
      val maxBound = if (bound.max < span.max) bound.max else span.max
      val rng = Interval(minBound, maxBound)
      var intvs: SortedMap[Interval, Int] = TreeMap[Interval, Int]()
      var gps: Seq[Graph[VD, ED]] = Seq[Graph[VD, ED]]()

      var numResults = 0
      val loop = new Breaks

      loop.breakable {
        intervals.foreach {
          case (k, v) =>
            if (numResults == 0 && k.contains(minBound)) {
              intvs += (k -> numResults)
              numResults = 1
              gps = gps :+ graphs(v)
            } else if (numResults > 0 && k.contains(maxBound)) {
              intvs += (k -> numResults)
              numResults += 1
              gps = gps :+ graphs(v)
              loop.break
            } else if (numResults > 0) {
              intvs += (k -> numResults)
              numResults += 1
              gps = gps :+ graphs(v)
            }
        }
      }

      new SnapshotGraph(rng, intvs, gps)
    } else
      null
  }

  //the result is another snapshotgraph
  //the resolution is in number of graphs to combine, regardless of the resolution of
  //the base data
  //there are no gaps in the output data, although some snapshots may have 0 vertices
  def aggregate(resolution: Int, sem: AggregateSemantics.Value, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): SnapshotGraph[VD, ED] = {
    var intvs: SortedMap[Interval, Int] = TreeMap[Interval, Int]()
    var gps: Seq[Graph[VD, ED]] = Seq[Graph[VD, ED]]()

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

    new SnapshotGraph(span, intvs, gps)
  }

  def partitionBy(pst: PartitionStrategyType.Value, runs: Int): SnapshotGraph[VD, ED] = {
    partitionBy(pst, runs, graphs.head.edges.partitions.size)
  }

  def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): SnapshotGraph[VD, ED] = {
    var numParts = if (parts > 0) parts else graphs.head.edges.partitions.size

    if (pst != PartitionStrategyType.None) {
      var result: SnapshotGraph[VD, ED] = new SnapshotGraph[VD, ED](span)
      //not changing the intervals, only the graphs at their indices
      //each partition strategy for SG needs information about the graph

      //use that strategy to partition each of the snapshots
      intervals.foreach {
        case (k, v) =>
          result = result.addSnapshot(k, graphs(v).partitionBy(PartitionStrategies.makeStrategy(pst, v, graphs.size, runs), numParts))
      }

      result
    } else
      this
  }

  //run PageRank on each contained snapshot
  def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): SnapshotGraph[Double, Double] = {
    var intvs: SortedMap[Interval, Int] = TreeMap[Interval, Int]()
    var gps: Seq[Graph[Double, Double]] = Seq[Graph[Double, Double]]()

    val iter: Iterator[(Interval, Int)] = intervals.iterator
    var numResults: Int = 0

    while (iter.hasNext) {
      val (k, v) = iter.next
      intvs += (k -> numResults)
      numResults += 1
      if (graphs(v).edges.isEmpty) {
        gps = gps :+ Graph[Double, Double](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
      } else {
        if (uni)
          gps = gps :+ UndirectedPageRank.run(Graph(graphs(v).vertices, graphs(v).edges.coalesce(1, true)), tol, resetProb, numIter)
        else
          //FIXME: this doesn't use the numIterations stop condition
          gps = gps :+ graphs(v).pageRank(tol, resetProb)
      }
    }

    new SnapshotGraph(span, intvs, gps)
  }

}

object SnapshotGraph {
  //this assumes that the data is in the dataPath directory, each time period in its own files
  //and the naming convention is nodes<time>.txt and edges<time>.txt
  //TODO: extend to be more flexible about input data such as arbitrary uniform intervals are supported
  final def loadData(dataPath: String, sc: SparkContext): SnapshotGraph[String, Int] = {
    var minYear: Int = Int.MaxValue
    var maxYear: Int = 0

    var source:scala.io.Source = null
    var fs:FileSystem = null

    //TODO: this is ugly, would prefer to use one method that handles both
    if (dataPath.startsWith("hdfs://")) {
        val pt:Path = new Path(dataPath + "/Span.txt")
    	val conf: Configuration = new Configuration()
    	conf.addResource(new Path("/localhost/hadoop/hadoop-2.6.0/etc/hadoop/core-site.xml"));
    	fs = FileSystem.get(conf)
	source = scala.io.Source.fromInputStream(fs.open(pt))
    } else {
        source = scala.io.Source.fromFile(dataPath + "/Span.txt")
    }

    val lines = source.getLines
    minYear = lines.next.toInt
    maxYear = lines.next.toInt
    source.close()      

    val span = Interval(minYear, maxYear)
    var years = 0
    var intvs: SortedMap[Interval, Int] = TreeMap[Interval, Int]()
    var gps: Seq[Graph[String, Int]] = Seq[Graph[String, Int]]()

    for (years <- minYear to maxYear) {
      val users: RDD[(VertexId, String)] = sc.textFile(dataPath + "/nodes/nodes" + years + ".txt").map(line => line.split(",")).map { parts =>
        if (parts.size > 1 && parts.head != "")
          (parts.head.toLong, parts(1).toString)
        else
          (0L, "Default")
      }
      var edges: RDD[Edge[Int]] = sc.emptyRDD
    
      if (dataPath.startsWith("hdfs://")) {  
            val ept:Path = new Path(dataPath + "/edges/edges" + years + ".txt")
      	    if (fs.exists(ept) && fs.getFileStatus(ept).getLen > 0) {
               //uses extended version of Graph Loader to load edges with attributes
               val tmp = years
               edges = GraphLoaderAddon.edgeListFile(sc, dataPath + "/edges/edges" + years + ".txt", true).edges
      	    } else {
              edges = sc.emptyRDD
      	    }
      } else {
      	    if ((new java.io.File(dataPath + "/edges/edges" + years + ".txt")).length > 0) {
               //uses extended version of Graph Loader to load edges with attributes
               val tmp = years
               edges = GraphLoaderAddon.edgeListFile(sc, dataPath + "/edges/edges" + years + ".txt", true).edges
      	    } else {
              edges = sc.emptyRDD
      	    }
      }

      val graph: Graph[String, Int] = Graph(users, EdgeRDD.fromEdges(edges))
      
      intvs += (Interval(years, years) -> (years - minYear))
      gps = gps :+ graph
    }
    new SnapshotGraph(span, intvs, gps)
  }
}
