//One multigraph
//Each vertex and edge has a value attribute associated with each time period
package edu.drexel.cs.dbgroup.graphxt

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import scala.reflect.ClassTag
import scala.util.control._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import org.apache.spark.graphx.impl.GraphXPartitionExtension._

import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.impl.EdgeRDDImpl
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.rdd._

import edu.drexel.cs.dbgroup.graphxt.util.MultifileLoad

class MultiGraph[VD: ClassTag, ED: ClassTag](sp: Interval, mp: SortedMap[Interval, Int], grs: Graph[Map[Int, VD], (ED, Int)]) extends TemporalGraph[VD, ED] {
  var span = sp
  var graphs: Graph[Map[Int, VD], (ED, Int)] = grs
  var intervals: SortedMap[Interval, Int] = mp

  def this(sp: Interval) = {
    this(sp, TreeMap[Interval, Int](), Graph[Map[Int, VD], (ED, Int)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD))
  }

  def size(): Int = intervals.size

  def numEdges(): Long = graphs.numEdges

  def numPartitions(): Int = {
    if (graphs.edges.isEmpty)
      0
    else
      graphs.edges.partitions.size
  }

  //FIXME: this is an arbitrary mapping right now, what should we return?
  def vertices: VertexRDD[VD] = graphs.vertices.mapValues { attr => attr.values.head }
  def edges: EdgeRDD[ED] = graphs.edges.mapValues { case e => e.attr._1 }

  def persist(newLevel: StorageLevel = MEMORY_ONLY): MultiGraph[VD, ED] = {
    //just persist the graph itself
    graphs.persist(newLevel)
    this
  }

  def unpersist(blocking: Boolean = true): MultiGraph[VD, ED] = {
    graphs.unpersist(blocking)
    this
  }

  //This operation is very inefficient. Adding one snapshot at a time is not recommended
  def addSnapshot(place: Interval, snap: Graph[VD, ED]): MultiGraph[VD, ED] = {
    //locate the position for this interval
    val iter: Iterator[Interval] = intervals.keysIterator
    var pos: Int = -1
    var found: Boolean = false
    var intvs: SortedMap[Interval, Int] = intervals

    val loop = new Breaks
    loop.breakable {
      while (iter.hasNext) {
        val nx: Interval = iter.next
        if (nx > place) {
          pos = intervals(nx) - 1
          found = true
          loop.break
        }
      }
    }

    //pos can be negative if there are no elements
    //or if this interval is the smallest of all
    //or if this interval is the lartest of all
    if (pos < 0) {
      if (found) {
        //put in position 0, move the rest to the right
        pos = 0
        intvs.map { case (k, v) => (k, v + 1) }
        intvs += (place -> pos)
      } else {
        //put in last
        intvs += (place -> intvs.size)
      }
    } else {
      //put in the specified position, move the rest to the right
      intvs.foreach { case (k, v) => if (v > pos) (k, v + 1) }
    }

    //join the vertices
    //cogroup produces two sequences
    //in our case the first sequence is a list of Map or empty if no previous data
    //and the second sequence is a list of VD or empty
    //because we are combining distinct rdds, each list should only have 1 element
    val verts = VertexRDD(graphs.vertices.cogroup(snap.vertices).map {
      case (k, (v1, v2)) =>
        if (v1.isEmpty) {
          (k, Map[Int, VD](pos -> v2.head))
        } else if (v2.isEmpty) {
          if (found) {
            (k, v1.head.map {
              case (ind, atr) =>
                if (ind > pos)
                  (ind + 1, atr)
                else
                  (ind, atr)
            })
          } else {
            (k, v1.head)
          }
        } else {
          //need to add this new element into the map at the index and move the others
          (k, v1.head.map {
            case (ind, atr) =>
              if (ind > pos)
                (ind + 1, atr)
              else
                (ind, atr)
          } + (pos -> v2.head))
        }
    })
    //it is understood that this snapshot is new so there shouldn't be any repeated edges
    //MG edges have an attribute and an index
    //but snapshot edges have just an attribute
    val edgs = EdgeRDD.fromEdges[(ED, Int), VD](graphs.edges.union(snap.edges.mapValues { e => (e.attr, pos) }) //.mapValues{
    //      case (srcid, dstid, (attr,Some(intervalindex))) => (srcid, dstid, (attr,intervalindex))
    //      case (srcid, dstid, attr) => (srcid, dstid, (attr,pos))
    //    })
    )
    //make the graph
    new MultiGraph[VD, ED](Interval(intvs.head._1.min, intvs.last._1.max), intvs, Graph(verts, edgs))
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
    if (pos >= 0 && pos < intervals.size) {
      val subg = graphs.subgraph(vpred = (vid, attr) => attr.keySet.contains(pos), epred = et => (et.attr._2 == pos))
      //need to strip the years from the vertices and edges
      val verts = VertexRDD[VD](subg.vertices.mapValues { mapattr => mapattr.values.head })
      val edgs = EdgeRDD.fromEdges[ED, VD](subg.edges.mapValues { case e => e.attr._1 })
      Graph(verts, edgs)
    } else
      null
  }

  def select(bound: Interval): MultiGraph[VD, ED] = {
    if (span.min == bound.min && span.max == bound.max) return this

    if (span.intersects(bound)) {
      val minBound = if (bound.min > span.min) bound.min else span.min
      val maxBound = if (bound.max < span.max) bound.max else span.max
      val rng = new Interval(minBound, maxBound)

      //need to re-compute the intervals map
      var intvs: SortedMap[Interval, Int] = intervals.filterKeys(x => if (x.max >= minBound && x.min <= maxBound) true else false)
      //the first interval should start no earlier than minBound
      if (intvs.firstKey.min < minBound) {
        //remove, reinsert
        var beg: Interval = intvs.firstKey
        val ind: Int = intvs(beg)
        intvs -= beg
        beg.min = minBound
        intvs += (beg -> ind)
      }
      //the last interval should end no later than maxBound
      if (intvs.lastKey.max > maxBound) {
        //remove, reinsert
        var end: Interval = intvs.lastKey
        val ind: Int = intvs(end)
        intvs -= end
        end.max = maxBound
        intvs += (end -> ind)
      }

      //that's how much we need to reduce all the selected vertex/edges indices by
      val oldst: Int = intvs(intvs.firstKey)
      val olden: Int = intvs(intvs.lastKey)
      val temp = intvs.values.toSet

      //now select the vertices and edges
      val subg = graphs.subgraph(
        vpred = (vid, attr) => !attr.keySet.intersect(temp).isEmpty,
        epred = et => temp.contains(et.attr._2))
      //now need to renumber vertex and edge intervals indices
      //indices outside of selected range get dropped
      val verts = VertexRDD(subg.vertices.mapValues((vid, attr) => attr.filterKeys(x => if (x >= oldst && x <= olden) true else false).map { case (k, v) => (k - oldst, v) }))
      val edges = EdgeRDD.fromEdges[(ED, Int), VD](subg.edges.mapValues(e => (e.attr._1, e.attr._2 - oldst)))

      //now need to renumber to start with 0 index
      intvs = intvs.mapValues(x => (x - oldst))

      println("Done with select....")
      new MultiGraph[VD, ED](rng, intvs, Graph(verts, edges))
    } else
      null
  }

  //the result is another MultiGraph
  //the resolution is in number of graphs to combine, regardless of the resolution of
  //the base data
  def aggregate(resolution: Int, sem: AggregateSemantics.Value, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): MultiGraph[VD, ED] = {
    //make new intervals
    var intvs: SortedMap[Interval, Int] = TreeMap[Interval, Int]()
    var nextind: Int = 0
    var minBound, maxBound: Int = 0

    //make a sequence of resolution sizes
    var seqs: Seq[Int] = Seq[Int]()
    val iter: Iterator[(Interval, Int)] = intervals.iterator
    while (iter.hasNext) {
      val (k, v) = iter.next
      minBound = k.min
      maxBound = k.max
      var yy = 0

      val loop = new Breaks
      loop.breakable {
        for (yy <- 1 to resolution - 1) {
          if (iter.hasNext) {
            val (k2, v2) = iter.next
            maxBound = k2.max
          } else {
            intvs += (Interval(minBound, maxBound) -> nextind)
            seqs = seqs :+ yy
            nextind += 1
            loop.break
          }
        }
        intvs += (Interval(minBound, maxBound) -> nextind)
        seqs = seqs :+ resolution
        nextind += 1
      }
    }

    val st: Int = intvs(intvs.firstKey)
    val en: Int = intvs(intvs.lastKey)
    //for each vertex, make a new list of indices
    //then filter out those vertices that have no indices
    val verts = VertexRDD(graphs.vertices.mapValues { (vid, attr) =>
      var tmp: Map[Int, Seq[VD]] = attr.toSeq.map { case (k, v) => (k / resolution, v) }.groupBy { case (k, v) => k }.mapValues { v => v.map { case (x, y) => y } }
      //tmp is now a map of (index, list(attr))
      if (sem == AggregateSemantics.Universal) {
        tmp = tmp.filter { case (k, v) => v.size == seqs(k) }
      }
      tmp.mapValues { v => v.reduce(vAggFunc) }.map(identity)
    }.filter { case (id, attr: Map[Int, VD]) => !attr.isEmpty })

    //TODO: maybe faster to filter out first based on vertices aggregated above
    var edges: EdgeRDD[(ED, Int)] = null
    if (sem == AggregateSemantics.Existential) {
      edges = EdgeRDD.fromEdges[(ED, Int), VD](graphs.edges.mapValues { e =>
        val (attr, ind) = e.attr
        //for each old index, replace with new index, i.e. for resolution 3, change 0,1,2 to 0
        (attr, ind / resolution)
      }.map(x => ((x.srcId, x.dstId, x.attr._2), x.attr._1)).reduceByKey(eAggFunc).map { x =>
        val (k, v) = x
        //key is srcid, dstid, resolution, value is attrib
        Edge(k._1, k._2, (v, k._3))
      })
    } else if (sem == AggregateSemantics.Universal) {
      edges = EdgeRDD.fromEdges[(ED, Int), VD](graphs.edges.mapValues { e =>
        val (attr, ind) = e.attr
        //for each old index, replace with new index, i.e. for resolution 3, change 0,1,2 to 0
        (attr, ind / resolution)
      }.map(x => ((x.srcId, x.dstId, x.attr._2), (x.attr._1, 1))).reduceByKey((x, y) => (eAggFunc(x._1, y._1), x._2 + y._2)).filter { case (k, (attr, cnt)) => cnt == resolution }.map { x =>
        val (k, v) = x
        //key is srcid, dstid, resolution, value is attrib and count
        Edge(k._1, k._2, (v._1, k._3))
      })
    }

    new MultiGraph[VD, ED](span, intvs, Graph(verts, edges))
  }

  def partitionBy(pst: PartitionStrategyType.Value, runs: Int): MultiGraph[VD, ED] = {
    partitionBy(pst, runs, graphs.edges.partitions.size)
  }

  def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): MultiGraph[VD, ED] = {
    var numParts = if (parts > 0) parts else graphs.edges.partitions.size

    if (pst != PartitionStrategyType.None) {
      //not changing the intervals
      new MultiGraph[VD, ED](span, intervals, graphs.partitionByExt(PartitionStrategies.makeStrategy(pst, 0, intervals.size, runs), numParts))
    } else
      this
  }

  //run pagerank on each interval
  def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): MultiGraph[Double, Double] = {
    if (uni)
      new MultiGraph[Double, Double](span, intervals, UndirectedPageRank.runCombined(graphs, intervals.size, tol, resetProb, numIter))
    else
      //not supported for now
      //TODO: implement this
      null
  }
  
  //run connected components on each interval
  def connectedComponents(): MultiGraph[VertexId, ED] = {
      new MultiGraph[VertexId, ED](span, intervals, ConnectedComponentsXT.runCombined(graphs, intervals.size))
  }
  
  //run shortestPaths on each interval
  def shortestPaths(landmarks: Seq[VertexId]): MultiGraph[ShortestPathsXT.SPMap, ED] = {
      new MultiGraph[ShortestPathsXT.SPMap, ED](span, intervals, ShortestPathsXT.runCombined(graphs, landmarks, intervals.size))
  }

}

object MultiGraph {
  def loadData(dataPath: String, startIndex: TimeIndex = 0, endIndex: TimeIndex = Int.MaxValue): MultiGraph[String, Int] = {
    //get the min and max year from file 
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

    var users: RDD[(VertexId,Map[Int,String])] = MultifileLoad.readNodes(dataPath, minYear, maxYear).flatMap{ x => 
      val yr = x._1.split('/').last.dropWhile(!_.isDigit).takeWhile(_.isDigit).toInt
      x._2.split("\n").map(line => line.split(","))
        .flatMap { parts =>
        if (parts.size > 1 && parts.head != "") {
          Some((parts.head.toLong, Map((yr - minYear) -> parts(1).toString)))
        } else None
      }
    }

    val span = Interval(minYear, maxYear)

    val edges: RDD[Edge[(Int, Int)]] = MultifileLoad.readEdges(dataPath, minYear, maxYear).flatMap{ x =>
      val tmp = x._1.split('/').last.dropWhile(!_.isDigit).takeWhile(_.isDigit).toInt
      x._2.split("\n").flatMap{ line =>
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          val srcId = lineArray(0).toLong
          val dstId = lineArray(0).toLong
          var attr = 0
          if (lineArray.length > 2)
            attr = lineArray(2).toInt
          if (srcId > dstId)
            Some(Edge(dstId, srcId, (attr, tmp - minYear)))
          else
            Some(Edge(srcId, dstId, (attr, tmp - minYear)))
        } else None
      }
    }
    
    val graph: Graph[Map[Int, String], (Int, Int)] = Graph(users,
      EdgeRDD.fromEdges[(Int, Int), Map[Int, String]](edges), Map[Int,String]())
    //graph.cache()

    var intvs: SortedMap[Interval, Int] = TreeMap[Interval, Int]()
    var xx: Int = 0
    for (xx <- 0 to maxYear) {
      intvs += (Interval(xx + minYear, xx + minYear) -> xx)
    }

    new MultiGraph[String, Int](span, intvs, graph)
  }

}
