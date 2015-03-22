//One multigraph
package edu.drexel.cs.dbgroup.graphxt

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import scala.reflect.ClassTag
import scala.util.control._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import org.apache.spark.graphx._
import org.apache.spark.graphx.GraphLoaderAddon
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.rdd._

//TODO: modify such that different partitioning strategies can be specified
class MultiGraph[VD: ClassTag, ED: ClassTag] (sp: Interval, mp: SortedMap[Interval, Int], grs: Graph[(VD, Seq[Int]),(ED,Int)]) extends Serializable {
  var span = sp
  var graphs:Graph[(VD,Seq[Int]),(ED,Int)] = grs
  var intervals:SortedMap[Interval, Int] = mp

//  def this(sp: Interval) = {
//    this(sp, TreeMap[Interval,Int](), Graph[(VD,List[Int]),(ED,Int)](sc.emptyRDD,sc.emptyRDD))
//  }

  def size():Int = intervals.size

  def persist(newLevel: StorageLevel = MEMORY_ONLY):Unit = {
    //just persist the graph itself
    graphs.persist(newLevel)
  }

  def unpersist(blocking: Boolean = true) = {
    graphs.unpersist(blocking)
  }

  //This operation is very inefficient. Adding one snapshot at a time is not recommended
/*
  def addSnapshot(place:Interval, snap:Graph[VD, ED]):Unit = {
    //locate the position for this interval
    val iter:Iterator[Interval] = intervals.keysIterator
    var pos:Int = -1
    var found:Boolean = false
    //this is not as efficient as binary search but the list is expected to be short

    val loop = new Breaks
    loop.breakable{
      while (iter.hasNext) {
        val nx:Interval = iter.next
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
        intervals.map { case (k,v) => (k, v+1) }
        intervals += (place -> pos)
      } else {
        //put in last
        intervals += (place -> intervals.size)
      }
    } else {
      //put in the specified position, move the rest to the right
      intervals.foreach { case (k,v) => if (v > pos) (k, v+1)}
    }

    //join the vertices
    //cogroup produces two sequences
    //in our case the first sequence is a list of VD,List(int) empty if no previous data
    //and the second sequence is a list of VD or empty
    //because we are combining distinct rdds, each list should only have 1 element
    val verts = VertexRDD(graphs.vertices.cogroup(snap.vertices).map{ case (k,(v1,v2)) =>
      if (v1.isEmpty) {
        (k,v2.head,List(pos))
      } else if (v2.isEmpty) {
        if (found) {
          (k,v1.head._1,v1.head._2.foreach { v => if (v > pos) v+1})
        } else {
          (k,v1.head._1,v1.head._2)
        }
      } else {
        val newl:List[Int] = v1.head._2.map {(v:Int) => if (v>pos) v+1} :+ pos
        (k,v1.head._1,newl.sorted)
      }
    })
    //it is understood that this snapshot is new so there shouldn't be any repeated edges
    //MG edges have an attribute and an index
    //but snapshot edges have just an attribute
    val edgs = EdgeRDD(graphs.edges.union(snap.edges).mapValues{
      case (srcid, dstid, (attr,Some(intervalindex))) => (srcid, dstid, (attr,intervalindex))
      case (srcid, dstid, attr) => (srcid, dstid, (attr,pos))
    })

    //make the graph
    graphs = Graph(verts,edgs)
  }
 */

  def getSnapshotByTime(time: Int): Graph[VD, ED] = {
    if (time >= span.min && time<= span.max) {
      //retrieve the position of the correct interval
      var position = -1
      val iter:Iterator[Interval] = intervals.keysIterator
      val loop = new Breaks
      loop.breakable {
        while (iter.hasNext) {
          val nx:Interval = iter.next
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
      val subg = graphs.subgraph(vpred = (vid,attr) => attr._2.contains(pos), epred = et => (et.attr._2 == pos))
      //need to strip the years from the vertices and edges
      val verts = VertexRDD[VD](subg.vertices.mapValues { (vid,attr) => attr._1})
      val edgs = EdgeRDD.fromEdges[ED,VD](subg.edges.mapValues {case e => e.attr._1})
      Graph(verts,edgs)
    } else
      null
  }

  def select(bound: Interval): MultiGraph[VD, ED] = {
    if (span.intersects(bound)) {
      val minBound = if (bound.min > span.min) bound.min else span.min
      val maxBound = if (bound.max < span.max) bound.max else span.max
      val rng = new Interval(minBound,maxBound)

      //need to re-compute the intervals map
      var intvs:SortedMap[Interval, Int] = intervals.filterKeys(x => if (x.max >= minBound && x.min <= maxBound) true else false)
      //the first interval should start no earlier than minBound
      if (intvs.firstKey.min < minBound) {
        //remove, reinsert
        var beg:Interval = intvs.firstKey
        val ind:Int = intvs(beg)
        intvs -= beg
        beg.min = minBound
        intvs += (beg -> ind)
      }
      //the last interval should end no later than maxBound
      if (intvs.lastKey.max > maxBound) {
        //remove, reinsert
        var end:Interval = intvs.lastKey
        val ind:Int = intvs(end)
        intvs -= end
        end.max = maxBound
        intvs += (end -> ind)
      }

      //that's how much we need to reduce all the selected vertex/edges indices by
      val oldst:Int = intvs(intvs.firstKey)

      //now select the vertices and edges
      val subg = graphs.subgraph(vpred = (vid,attr) => !attr._2.intersect(Seq(intvs.values)).isEmpty, epred = et => !Seq(intvs.values).intersect(Seq(et.attr._2)).isEmpty)
      //now need to renumber vertex and edge intervals indices
      val verts = VertexRDD(subg.vertices.mapValues((vid,attr) => (attr._1,attr._2.map(x => (x-oldst)))))
      val edges = EdgeRDD.fromEdges[(ED,Int),VD](subg.edges.mapValues(e => (e.attr._1,e.attr._2-oldst)))

      //now need to renumber to start with 0 index
      intvs = intvs.mapValues(x => (x-oldst))

      new MultiGraph[VD,ED](rng, intvs, Graph(verts,edges))
    } else
      null
  }

  //the result is another MultiGraph
  //the resolution is in number of graphs to combine, regardless of the resolution of
  //the base data
  //TODO: add error checking and boundary conditions handling
  //TODO: how to aggregate the vertex/edge properties?
  def aggregate(resolution: Int, sem: AggregateSemantics.Value): MultiGraph[VD,ED] = {
    //make new intervals
    var intvs:SortedMap[Interval, Int] = TreeMap[Interval,Int]()
    var nextind:Int = 0
    var minBound,maxBound: Int = 0

    //make a sequence of resolution size lists, i.e. (0,1,2),(3,4,5)
    var seqs: Seq[Seq[Int]] = Seq[Seq[Int]]()
    val iter:Iterator[(Interval,Int)] = intervals.iterator
    while (iter.hasNext) {
      val (k,v) = iter.next
      var sq:Seq[Int] = Seq(v)
      minBound = k.min
      maxBound = k.max
      var yy = 0

      val loop = new Breaks
      loop.breakable{
        for (yy <- 1 to resolution-1) {
          if (iter.hasNext) {
            val (k,v) = iter.next
          } else {
            loop.break
          }

          maxBound = k.max
          sq = sq :+ v
        }
        intvs += (Interval(minBound,maxBound) -> nextind)
        seqs = seqs :+ sq
        nextind += 1
      }
    }

    //for each vertex, make a new list of indices
    //then filter out those vertices that have no indices
    val verts = VertexRDD(graphs.vertices.mapValues{ (vid,attr) =>
      val (attr1,lst) = attr
      var newlst:Seq[Int] = Seq()
      seqs.zipWithIndex.foreach {s =>
        val (seq,i) = s
        if (sem == AggregateSemantics.Existential) {
          //for each sequence of ints, put the new index if any present
          if (!lst.intersect(seq).isEmpty)
            newlst = newlst :+ i
        } else if (sem == AggregateSemantics.Universal) {
          if (lst.intersect(seq).size == resolution)
            newlst = newlst :+ i
        }
      }
      (attr1,newlst)
    }.filter{case (id, (attr,lst)) => !lst.isEmpty})

    //FIXME: aggregate edge attribute ED too
    var edges:EdgeRDD[(ED,Int)] = null
    if (sem == AggregateSemantics.Existential) {
      edges = EdgeRDD.fromEdges[(ED,Int),VD](graphs.edges.mapValues{ e =>
        val (attr,ind) = e.attr
        //for each old index, replace with new index, i.e. for resolution 3, change 0,1,2 to 0
        (attr,ind/resolution)
      }.distinct)
    } else if (sem == AggregateSemantics.Universal) {
      edges = EdgeRDD.fromEdges[(ED,Int),VD](graphs.edges.mapValues{ e =>
        val (attr,ind) = e.attr
        //for each old index, replace with new index, i.e. for resolution 3, change 0,1,2 to 0
        (attr,ind/resolution)
      }.map(x => ((x.srcId, x.dstId, x.attr._2), (x.attr._1,1))).reduceByKey((x,y) => (x._1,x._2+y._2)).filter{ case (k,(attr,cnt)) => cnt==resolution}.map{x => 
        val (k,v) = x
        //key is srcid, dstid, resolution, value is attrib and count
        Edge(k._1, k._2, (v._1, k._3))
      })
    }

    new MultiGraph[VD,ED](span, intvs, Graph(verts,edges))
  }

  //run pagerank on each interval
  def pageRank(tol: Double, resetProb: Double = 0.15): MultiGraph[Seq[Double],Double] = {
    new MultiGraph[Seq[Double],Double](span,intervals,UndirectedPageRank.runCombined(graphs,intervals.size,tol,resetProb))
  }

  def partitionBy(pst: PartitionStrategyType.Value):MultiGraph[VD,ED] = {
    if (pst != PartitionStrategyType.None) {
      //not changing the intervals
      new MultiGraph[VD,ED](span,intervals,graphs.partitionBy(PartitionStrategies.makeStrategy(pst,0,intervals.size)))
    } else
      this
  }

}

object MultiGraph {

  def loadGraph(dataPath: String, sc:SparkContext): MultiGraph[String,Int] = {
    var minYear:Int = Int.MaxValue
    var maxYear:Int = 0

    val users = sc.textFile(dataPath + "/Node-ID.txt").map { line =>
      val fields = line.split(",")
      val tl = fields.tail.tail
      val miny:Int = tl.min.toInt
      val maxy:Int = tl.max.toInt
      minYear = math.min(minYear,miny)
      maxYear = math.max(maxYear,maxy)
      (fields.head.toLong, fields.tail)
    }

    //GraphLoader.edgeListFile does not allow for additional attributes besides srcid/dstid
    //so use an extended version instead which does (but assumes the attr is an int)
    //the edges are in years, need to translate into indices
    val edges = GraphLoaderAddon.edgeListFile(sc, dataPath + "/Edge-ID.txt").mapEdges(e => (1,e.attr-minYear))

    //in the source data the vertex attribute list is the name followed by years
    //we need to transform that into a tuple of name,List(indices)
    val graph:Graph[(String,Seq[Int]),(Int,Int)] = edges.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => (attrList.head,attrList.tail.map(x => x.toInt - minYear).toSeq)
      case (uid, deg, None) => null
    }

    //make a list of intervals (here, years) and indices
    var intvs:SortedMap[Interval, Int] = TreeMap[Interval,Int]()
    var xx:Int = 0
    for (xx <- 0 to maxYear) {
      intvs += (Interval(xx+minYear,xx+minYear) -> xx)
    }

    new MultiGraph[String,Int](Interval(minYear,maxYear), intvs, graph)
  }
}

class MultiGraphPTest {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "SnapshotGraph Project", 
      "/Users/vzaychik/spark-1.2.1",
      List("target/scala-2.10/simple-project_2.10-1.0.jar"))

    var testGraph = MultiGraph.loadGraph(args(0), sc)
    val interv = new Interval(1980, 2015)
    val aggregate = testGraph.select(interv).aggregate(5, AggregateSemantics.Existential)
    //there should be 8 results
    println("total number of results after aggregation: " + aggregate.size)
    //let's run pagerank on the aggregate now
    val ranks = aggregate.pageRank(0.0001)
    println("done")
  }

}
