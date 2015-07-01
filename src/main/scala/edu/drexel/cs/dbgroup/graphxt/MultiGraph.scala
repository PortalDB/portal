//One multigraph
//Each vertex and edge has a value attribute associated with each time period
package edu.drexel.cs.dbgroup.graphxt

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import scala.collection.breakOut
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

class MultiGraph[VD: ClassTag, ED: ClassTag](sp: Interval, res: Int, mp: SortedMap[Interval, TimeIndex], grs: Graph[Map[TimeIndex, VD], (TimeIndex, ED)]) extends TemporalGraph[VD, ED] {
  val span = sp
  val resolution = res
  val graphs: Graph[Map[TimeIndex, VD], (TimeIndex, ED)] = grs
  val intervals: SortedMap[Interval, TimeIndex] = mp

  /** Default constructor is provided to support serialization */
  protected def this() = this(null, 0, null, null)

  def this(sp: Interval) = {
    this(sp, 1, TreeMap[Interval, Int](), Graph[Map[Int, VD], (TimeIndex, ED)](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD))
  }

  override def size(): Int = intervals.size

  override def numEdges(): Long = graphs.numEdges

  override def numPartitions(): Int = {
    if (graphs.edges.isEmpty)
      0
    else
      graphs.edges.partitions.size
  }

  override def vertices: VertexRDD[Map[TimeIndex, VD]] = graphs.vertices

  override def verticesFlat: VertexRDD[(TimeIndex, VD)] = {
    VertexRDD(graphs.vertices.flatMap(v => v._2.map(x => (v._1, (x._1, x._2)))))
  }

  override def edges: EdgeRDD[Map[TimeIndex, ED]] = {
    EdgeRDD.fromEdges[Map[TimeIndex, ED], VD](graphs.edges.map(x => ((x.srcId, x.dstId), Map[TimeIndex, ED](x.attr._1 -> x.attr._2)))
    .reduceByKey((a: Map[TimeIndex, ED], b: Map[TimeIndex, ED]) => a ++ b)
    .map(x => Edge(x._1._1, x._1._2, x._2))
    )
  }

  override def edgesFlat: EdgeRDD[(TimeIndex, ED)] = graphs.edges

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): MultiGraph[VD, ED] = {
    //just persist the graph itself
    graphs.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): MultiGraph[VD, ED] = {
    graphs.unpersist(blocking)
    this
  }

  //This operation is very inefficient. Adding one snapshot at a time is not recommended
  override def addSnapshot(place: Interval, snap: Graph[VD, ED]): MultiGraph[VD, ED] = {
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
          (k, Map[TimeIndex, VD](pos -> v2.head))
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
    val edgs = EdgeRDD.fromEdges[(TimeIndex, ED), VD](graphs.edges.union(snap.edges.mapValues { e => (pos, e.attr) }))

    //make the graph
    new MultiGraph[VD, ED](Interval(intvs.head._1.min, intvs.last._1.max), resolution, intvs, Graph(verts, edgs))
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
    if (pos >= 0 && pos < intervals.size) {
      val subg = graphs.subgraph(vpred = (vid, attr) => attr.keySet.contains(pos), epred = et => (et.attr._1 == pos))
      //need to strip the years from the vertices and edges
      val verts = VertexRDD[VD](subg.vertices.mapValues { mapattr => mapattr.values.head })
      val edgs = EdgeRDD.fromEdges[ED, VD](subg.edges.mapValues { case e => e.attr._2 })
      Graph(verts, edgs)
    } else
      null
  }

  override def select(bound: Interval): MultiGraph[VD, ED] = {
    if (span.min == bound.min && span.max == bound.max) return this

    if (span.intersects(bound)) {
      val minBound = if (bound.min > span.min) bound.min else span.min
      val maxBound = if (bound.max < span.max) bound.max else span.max
      val rng = new Interval(minBound, maxBound)

      //need to re-compute the intervals map
      var intvs: SortedMap[Interval, TimeIndex] = intervals.filterKeys(x => if (x.max >= minBound && x.min <= maxBound) true else false)
      //the first interval should start no earlier than minBound
      if (intvs.firstKey.min < minBound) {
        //remove, reinsert
        var beg: Interval = intvs.firstKey
        val ind: TimeIndex = intvs(beg)
        intvs -= beg
        beg.min = minBound
        intvs += (beg -> ind)
      }
      //the last interval should end no later than maxBound
      if (intvs.lastKey.max > maxBound) {
        //remove, reinsert
        var end: Interval = intvs.lastKey
        val ind: TimeIndex = intvs(end)
        intvs -= end
        end.max = maxBound
        intvs += (end -> ind)
      }

      //that's how much we need to reduce all the selected vertex/edges indices by
      val oldst: TimeIndex = intvs(intvs.firstKey)
      val olden: TimeIndex = intvs(intvs.lastKey)
      val temp = intvs.values.toSet

      //now select the vertices and edges
      val subg = graphs.subgraph(
        vpred = (vid, attr) => !attr.keySet.intersect(temp).isEmpty,
        epred = et => temp.contains(et.attr._1))
      //now need to renumber vertex and edge intervals indices
      //indices outside of selected range get dropped
      val verts = VertexRDD(subg.vertices.mapValues((vid, attr) => attr.filterKeys(x => if (x >= oldst && x <= olden) true else false).map { case (k, v) => (k - oldst, v) }))
      val edges = EdgeRDD.fromEdges[(TimeIndex, ED), VD](subg.edges.mapValues(e => (e.attr._1 - oldst, e.attr._2)))

      //now need to renumber to start with 0 index
      intvs = intvs.mapValues(x => (x - oldst))

      println("Done with select....")
      new MultiGraph[VD, ED](rng, resolution, intvs, Graph(verts, edges))
    } else
      null
  }

  //the result is another MultiGraph
  //the resolution is in number of graphs to combine, regardless of the resolution of
  //the base data
  override def aggregate(resolution: Int, sem: AggregateSemantics.Value, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): MultiGraph[VD, ED] = {
    //make new intervals
    var intvs: SortedMap[Interval, TimeIndex] = TreeMap[Interval, TimeIndex]()
    var nextind: Int = 0
    var minBound, maxBound: Int = 0

    //make a sequence of resolution sizes
    var seqs: Seq[Int] = Seq[Int]()
    val iter: Iterator[(Interval, TimeIndex)] = intervals.iterator
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

    val st: TimeIndex = intvs(intvs.firstKey)
    val en: TimeIndex = intvs(intvs.lastKey)
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
    var edges: EdgeRDD[(TimeIndex, ED)] = null
    if (sem == AggregateSemantics.Existential) {
      edges = EdgeRDD.fromEdges[(TimeIndex, ED), VD](graphs.edges.mapValues { e =>
        val (ind, attr) = e.attr
        //for each old index, replace with new index, i.e. for resolution 3, change 0,1,2 to 0
        (ind / resolution, attr)
      }.map(x => ((x.srcId, x.dstId, x.attr._1), x.attr._2)).reduceByKey(eAggFunc).map { x =>
        val (k, v) = x
        //key is srcid, dstid, resolution, value is attrib
        Edge(k._1, k._2, (k._3, v))
      })
    } else if (sem == AggregateSemantics.Universal) {
      edges = EdgeRDD.fromEdges[(TimeIndex, ED), VD](graphs.edges.mapValues { e =>
        val (ind, attr) = e.attr
        //for each old index, replace with new index, i.e. for resolution 3, change 0,1,2 to 0
        (ind / resolution, attr)
      }.map(x => ((x.srcId, x.dstId, x.attr._1), (x.attr._2, 1))).reduceByKey((x, y) => (eAggFunc(x._1, y._1), x._2 + y._2)).filter { case (k, (attr, cnt)) => cnt == resolution }.map { x =>
        val (k, v) = x
        //key is srcid, dstid, resolution, value is attrib and count
        Edge(k._1, k._2, (k._3, v._1))
      })
    }

    new MultiGraph[VD, ED](span, resolution * this.resolution, intvs, Graph(verts, edges))
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int): MultiGraph[VD, ED] = {
    partitionBy(pst, runs, graphs.edges.partitions.size)
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): MultiGraph[VD, ED] = {
    var numParts = if (parts > 0) parts else graphs.edges.partitions.size

    if (pst != PartitionStrategyType.None) {
      //not changing the intervals
      new MultiGraph[VD, ED](span, resolution, intervals, graphs.partitionByExt(PartitionStrategies.makeStrategy(pst, 0, intervals.size, runs), numParts))
    } else
      this
  }

  override def degrees: VertexRDD[Map[TimeIndex, Int]] = {
    def mergedFunc(a:Map[TimeIndex,Int], b:Map[TimeIndex,Int]): Map[TimeIndex,Int] = {
      a ++ b.map { case (index,count) => index -> (count + a.getOrElse(index,0)) }
    }

    //compute degree of each vertex for each interval
    //this should produce a map from interval to degree for each vertex
    //since edge is treated by MultiGraph as undirectional, can use the edge markings
    graphs.aggregateMessages[Map[TimeIndex,Int]](
      ctx => {
        ctx.sendToSrc(Map(ctx.attr._1 -> 1))
        ctx.sendToDst(Map(ctx.attr._1 -> 1))
      },
      mergedFunc,
      TripletFields.All)
  }

  override def union(other: TemporalGraph[VD, ED]): TemporalGraph[VD, ED] = {
    var grp2: MultiGraph[VD, ED] = other match {
      case grph: MultiGraph[VD, ED] => grph
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

    //now the graph itself. if we just do a union, then we'll have repeated vertices
    //and potentially edges, so have to reduce
    val newverts: VertexRDD[Map[TimeIndex, VD]] = VertexRDD((graphs.vertices union grp2.vertices).reduceByKey((a: Map[TimeIndex, VD], b: Map[TimeIndex, VD]) => a ++ b))
    val newedges: EdgeRDD[(TimeIndex, ED)] = EdgeRDD.fromEdges[(TimeIndex, ED), Map[TimeIndex, VD]]((graphs.edges union grp2.graphs.edges).distinct)
    val newgraph: Graph[Map[TimeIndex, VD], (TimeIndex, ED)] = Graph[Map[TimeIndex, VD], (TimeIndex, ED)](newverts, newedges)

    new MultiGraph[VD, ED](Interval(minBound, maxBound), resolution, mergedIntervals, newgraph)
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, Map[TimeIndex, U])])(mapFunc: (VertexId, TimeIndex, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val newgrp: Graph[Map[TimeIndex, VD2], (TimeIndex, ED)] = graphs.outerJoinVertices(other){ (id: VertexId, attr1: Map[TimeIndex, VD], attr2: Option[Map[TimeIndex, U]]) =>
      if (attr2.isEmpty)
        attr1.map(x => (x._1, mapFunc(id, x._1, x._2, None)))
      else
        attr1.map(x => (x._1, mapFunc(id, x._1, x._2, attr2.get.get(x._1))))
    }

    new MultiGraph[VD2, ED](span, resolution, intervals, newgrp)
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, TimeIndex, VD) => VD2)(implicit eq: VD =:= VD2 = null): TemporalGraph[VD2, ED] = {
    val newgrp: Graph[Map[TimeIndex, VD2], (TimeIndex, ED)] = graphs.mapVertices{ (id: VertexId, attr: Map[TimeIndex, VD]) =>
      attr.map(x => (x._1, map(id, x._1, x._2)))
    }
    new MultiGraph[VD2, ED](span, resolution, intervals, newgrp)
  }

  override def mapEdges[ED2: ClassTag](map: (Edge[ED], TimeIndex) => ED2): TemporalGraph[VD, ED2] = {
    val newgrp: Graph[Map[TimeIndex, VD], (TimeIndex, ED2)] = graphs.mapEdges{ e: Edge[(TimeIndex, ED)] =>
      (e.attr._1, map(Edge(e.srcId, e.dstId, e.attr._2), e.attr._1))
    }

    new MultiGraph[VD, ED2](span, resolution, intervals, newgrp)
  }

  override def pregel[A: ClassTag]
     (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
       sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): TemporalGraph[VD, ED] = {
    //because we run for all time instances at the same time,
    //need to convert programs and messages to the map form
    val initM: Map[TimeIndex, A] = (for(i <- 0 to intervals.size) yield (i -> initialMsg))(breakOut)
    def vertexP(id: VertexId, attr: Map[TimeIndex, VD], msg: Map[TimeIndex, A]): Map[TimeIndex, VD] = {
      var vals = attr
      msg.foreach {x =>
        val (k,v) = x
        if (vals.contains(k)) {
          vals = vals.updated(k, vprog(id, vals(k), v))
        }
      }
      vals
    }
    def sendMsgC(edge: EdgeTriplet[Map[TimeIndex, VD], (TimeIndex, ED)]): Iterator[(VertexId, Map[TimeIndex, A])] = {
      val et = new EdgeTriplet[VD, ED]
      et.srcId = edge.srcId
      et.dstId = edge.dstId
      et.srcAttr = edge.srcAttr.get(edge.attr._1).get
      et.dstAttr = edge.dstAttr.get(edge.attr._1).get
      et.attr = edge.attr._2
      //this returns Iterator[(VertexId, A)], but we need
      //Iterator[(VertexId, Map[TimeIndex, A])]
      sendMsg(et).map(x => (x._1, Map[TimeIndex, A](edge.attr._1 -> x._2)))
    }
    def mergeMsgC(a: Map[TimeIndex, A], b: Map[TimeIndex, A]): Map[TimeIndex, A] = {
      (a.keySet ++ b.keySet).map { k =>
        k -> mergeMsg(a.getOrElse(k, defValue), b.getOrElse(k, defValue))
      }.toMap
    }

    val newgrp: Graph[Map[TimeIndex, VD], (TimeIndex, ED)] = Pregel(graphs, initM, maxIterations, activeDirection)(vertexP, sendMsgC, mergeMsgC)
    new MultiGraph[VD, ED](span, resolution, intervals, newgrp)
  }

  //run pagerank on each interval
  def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): MultiGraph[Double, Double] = {
    if (uni)
      new MultiGraph[Double, Double](span, resolution, intervals, UndirectedPageRank.runCombined(graphs, intervals.size, tol, resetProb, numIter))
    else
      //not supported for now
      //TODO: implement this
      null
  }
  
  //run connected components on each interval
  def connectedComponents(): MultiGraph[VertexId, ED] = {
      new MultiGraph[VertexId, ED](span, resolution, intervals, ConnectedComponentsXT.runCombined(graphs, intervals.size))
  }
  
  //run shortestPaths on each interval
  def shortestPaths(landmarks: Seq[VertexId]): MultiGraph[ShortestPathsXT.SPMap, ED] = {
      new MultiGraph[ShortestPathsXT.SPMap, ED](span, resolution, intervals, ShortestPathsXT.runCombined(graphs, landmarks, intervals.size))
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
      val (filename, line) = x
      val yr = filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_.isDigit).toInt
      val parts = line.split(",")
      if (parts.size > 1 && parts.head != "") {
        Some((parts.head.toLong, Map((yr - minYear) -> parts(1).toString)))
      } else None
    }

    val span = Interval(minYear, maxYear)

    val edges = GraphLoaderAddon.edgeListFiles(MultifileLoad.readEdges(dataPath, minYear, maxYear), minYear, true).edges

    val graph: Graph[Map[Int, String], (Int, Int)] = Graph(users, edges, Map[Int,String]())

    var intvs: SortedMap[Interval, Int] = TreeMap[Interval, Int]()
    var xx: Int = 0
    for (xx <- 0 to maxYear) {
      intvs += (Interval(xx + minYear, xx + minYear) -> xx)
    }

    new MultiGraph[String, Int](span, 1, intvs, graph)
  }

}
