//For one graph per year (spanshots):
package edu.drexel.cs.dbgroup.graphxt

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import scala.reflect.ClassTag
import scala.util.control._

import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd._

class SnapshotGraph[VD: ClassTag, ED: ClassTag] (sp: Interval) extends Serializable {
  var span = sp
  var graphs:Seq[Graph[VD,ED]] = Seq[Graph[VD,ED]]()
  var intervals:SortedMap[Interval, Int] = TreeMap[Interval,Int]()
  
  //Note: this kind of breaks the normal spark/graphx paradigm of returning
  //the new object with the change rather than making the change in place
  //intervals are assumed to be nonoverlapping
  //Note: snapshots should be added from earliest to latest for performance reasons
  //TODO: more error checking on boundary conditions
  def addSnapshot(place:Interval, snap:Graph[VD, ED]): Unit = {
    val iter:Iterator[Interval] = intervals.keysIterator
    var pos:Int = -1
    var found:Boolean = false
    
    // create a Breaks object (to break out of a loop)
    val loop = new Breaks
    
    loop.breakable{
      //this is not as efficient as binary search but the list is expected to be short
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
    //or if this interval is the largest of all
    if (pos < 0) { 
      if (found) {
        //put in position 0, move the rest to the right
        pos = 0
        intervals.map { case (k,v) => (k, v+1) }
        intervals += (place -> pos)
        graphs = snap +: graphs
      } else {
        //put in last
        intervals += (place -> graphs.size)
        graphs = graphs :+ snap
      }
    } else {
      //put in the specified position, move the rest to the right
      intervals.foreach { case (k,v) => if (v > pos) (k, v+1)}
      val (st,en) = graphs.splitAt(pos-1)
      graphs = (st ++ (snap +: en))
    }
    //FIXME? Will this cause unnecessary re-partitioning?
    snap.partitionBy(new YearPartitionStrategy(place.min-span.min, span.max-span.min))
  }

  def getSnapshotByTime(time: Int): Graph[VD, ED] = {
    if (time >= span.min && time<= span.max) {
      //retrieve the position of the correct interval
      var position = -1
      val iter:Iterator[Interval] = intervals.keysIterator
      val loop = new Breaks
      
      loop.breakable{
        while (iter.hasNext) {
          val nx:Interval = iter.next
          if (nx.contains(time)) {
            //TODO: error checking if this key-value doesn't exist
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
      val rng = new Interval(minBound,maxBound)
      var result:SnapshotGraph[VD,ED] = new SnapshotGraph[VD,ED](rng)

      var numResults = 0
      val loop = new Breaks
      
      loop.breakable{
        intervals.foreach {
          case (k,v) =>
          if (numResults == 0 && k.contains(minBound)) {
            numResults = 1
            result.addSnapshot(k,graphs(v))
          } else if (numResults > 0 && k.contains(maxBound)) {
            numResults += 1
            result.addSnapshot(k,graphs(v))
            loop.break
          } else if (numResults > 0) {
            numResults += 1
            result.addSnapshot(k,graphs(v))
          }
        }
      }

      result
    } else
      null
  }

  //the result is another snapshotgraph
  //the resolution is in number of graphs to combine, regardless of the resolution of
  //the base data
  //there are no gaps in the output data, although some snapshots may have 0 vertices
  //TODO: add error checking and boundary conditions handling
  def aggregate(resolution: Int, sem: AggregateSemantics.Value): SnapshotGraph[VD,ED] = {
    var result:SnapshotGraph[VD,ED] = new SnapshotGraph[VD,ED](span)
    val iter:Iterator[(Interval,Int)] = intervals.iterator
    var minBound,maxBound:Int = 0;
    
    while (iter.hasNext) {
      val (k,v) = iter.next
      minBound = k.min
      //take resolution# of consecutive graphs, combine them according to the semantics
      var firstVRDD:VertexRDD[VD] = graphs(v).vertices
      var firstERDD:EdgeRDD[ED] = graphs(v).edges
      var yy = 0
      //FIXME: what if there is not an evenly divisible number of graphs
      //add handling for that - the last one just gets however many it gets
      for (yy <- 1 to resolution-1) {
        val (k,v) = iter.next
        if (sem == AggregateSemantics.Existential) {
          firstVRDD = VertexRDD(firstVRDD.union(graphs(v).vertices).distinct)
          firstERDD = EdgeRDD.fromEdges[ED,VD](firstERDD.union( graphs(v).edges ).distinct)
        } else if (sem == AggregateSemantics.Universal) {
          firstVRDD = VertexRDD(firstVRDD.intersection(graphs(v).vertices).distinct)
          firstERDD = EdgeRDD.fromEdges[ED,VD](firstERDD.intersection(graphs(v).edges).distinct)
        }
        maxBound = k.max
      }

      result.addSnapshot(new Interval(minBound,maxBound), Graph(firstVRDD,firstERDD))
    }
    result
  }

  //run PageRank on each contained snapshot
  def pageRank(tol: Double): SnapshotGraph[Double,Double] = {
    var result:SnapshotGraph[Double,Double] = new SnapshotGraph[Double,Double](span)
    val iter:Iterator[(Interval,Int)] = intervals.iterator
    
    while (iter.hasNext) {
      val (k,v) = iter.next
      result.addSnapshot(k,graphs(v).pageRank(tol))
    }
    
    result
  }
}

