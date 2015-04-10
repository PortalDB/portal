//For one graph per year (spanshots):
package edu.drexel.cs.dbgroup.graphxt

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import scala.reflect.ClassTag
import scala.util.control._

import org.apache.spark.SparkContext
import org.apache.spark.Partition
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd.PairRDDFunctions

class SnapshotGraphParallel[VD: ClassTag, ED: ClassTag] (sp: Interval, gps: Seq[Graph[VD,ED]], invs: SortedMap[Interval, Int]) extends Serializable {
  var span = sp
  var graphs:Seq[Graph[VD,ED]] = gps//Seq[Graph[VD,ED]]()
  var intervals:SortedMap[Interval, Int] = invs//TreeMap[Interval,Int]()
 
  //constructor overloading
  def this(sp:Interval) = { 
    this(sp, Seq[Graph[VD,ED]](), TreeMap[Interval,Int]())
  }
  
  def size(): Int = { graphs.size }

  def persist(newLevel: StorageLevel = MEMORY_ONLY):Unit = {
    //persist each graph
    graphs.map(_.persist(newLevel))
  }

  def unpersist(blocking: Boolean = true) = {
    graphs.map(_.unpersist(blocking))
  }

  def numEdges():Long = {
    val startAsMili = System.currentTimeMillis()
    val result = graphs.map(_.numEdges).reduce(_ + _)
    val endAsMili = System.currentTimeMillis()
    val finalAsMili = endAsMili - startAsMili
    println("total number of graphs: " + graphs.size)    
    println("Time to count edges in SnapshotGraphParallel: " + finalAsMili + "ms")
    result
  }

  def numPartitions():Int = {
    val startAsMili = System.currentTimeMillis()
    val result = graphs.map(_.edges.partitions.size).reduce(_ + _)
    val endAsMili = System.currentTimeMillis()
    val finalAsMili = endAsMili - startAsMili
    println("Time to count numParts in SnapshotGraphParallel: " + finalAsMili + "ms")
    result
  }

  //Note: this kind of breaks the normal spark/graphx paradigm of returning
  //the new object with the change rather than making the change in place
  //intervals are assumed to be nonoverlapping
  //Note: snapshots should be added from earliest to latest for performance reasons
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

  //since SnapshotGraphParallels are involatile (except with add)
  //the result is a new SnapshotGraphParallel
  def select(bound: Interval): SnapshotGraphParallel[VD, ED] = {
    if(!span.intersects(bound)){
      null
    }
    
    val minBound = if (bound.min > span.min) bound.min else span.min
    val maxBound = if (bound.max < span.max) bound.max else span.max
      
    //TODO: factor in boundary modifications for aggregated SnapshotGraphs
    var selectStart = math.abs(span.min - minBound)
    var selectStop = maxBound - span.min + 1
    
    var intervalFilter:SortedMap[Interval, Int] = intervals.slice(selectStart, selectStop)
    val newseq:Seq[Graph[VD,ED]] = intervalFilter.map(x => graphs(x._2)).toSeq
    val result = new SnapshotGraphParallel(span, newseq, intervals)
      
    result
  }

  //the result is another SnapshotGraphParallel
  //the resolution is in number of graphs to combine, regardless of the resolution of
  //the base data
  //there are no gaps in the output data, although some snapshots may have 0 vertices
  def aggregate(resolution: Int, sem: AggregateSemantics.Value, vAggFunc: (VD,VD) => VD, eAggFunc: (ED,ED) => ED): SnapshotGraphParallel[VD,ED] = {
    var result:SnapshotGraphParallel[VD,ED] = new SnapshotGraphParallel[VD,ED](span)
    val iter:Iterator[(Interval,Int)] = intervals.iterator
    var minBound,maxBound:Int = 0;
    
    while (iter.hasNext) {
      val (k,v) = iter.next
      minBound = k.min
      maxBound = k.max
      //take resolution# of consecutive graphs, combine them according to the semantics
      var firstVRDD:RDD[(VertexId, VD)] = graphs(v).vertices
      var firstERDD:RDD[Edge[ED]] = graphs(v).edges
      var yy = 0
      var width = 0
      
      val loop = new Breaks
      loop.breakable{
        for (yy <- 1 to resolution-1) {
        
          if(iter.hasNext){
            val (k,v) = iter.next
            firstVRDD = firstVRDD.union( graphs(v).vertices )
            firstERDD = firstERDD.union( graphs(v).edges )
            maxBound = k.max
          } else{
            width = yy-1
            loop.break
          }
        }
      }
      if (width == 0) width = resolution-1

      if (sem == AggregateSemantics.Existential) {
        result.addSnapshot(Interval(minBound,maxBound), Graph(VertexRDD(firstVRDD.reduceByKey(vAggFunc)), EdgeRDD.fromEdges[ED,VD](firstERDD.map(e => ((e.srcId,e.dstId),e.attr)).reduceByKey(eAggFunc).map { x => 
          val (k,v) = x
          Edge(k._1, k._2, v) })))
      } else if (sem == AggregateSemantics.Universal) {
        result.addSnapshot(Interval(minBound,maxBound), 
          Graph(firstVRDD.map(x => (x._1, (x._2, 1))).reduceByKey((x,y) => (vAggFunc(x._1,y._1),x._2+y._2)).filter(x => x._2._2 > width).map{x => 
            val (k,v) = x
            (k, v._1)}
            ,
            EdgeRDD.fromEdges[ED,VD](firstERDD.map(e => ((e.srcId,e.dstId),(e.attr,1))).reduceByKey((x,y) => (eAggFunc(x._1,y._1),x._2+y._2)).filter(x => x._2._2 > width).map { x =>
          val (k,v) = x
          Edge(k._1, k._2, v._1) }
        )))
      }
    }
    result
  }

  //run PageRank on each contained snapshot
  def pageRank(tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): SnapshotGraphParallel[Double,Double] = {
    def safePagerank(grp: Graph[VD,ED]):Graph[Double,Double] = {
      if (grp.edges.isEmpty){
        Graph[Double,Double](ProgramContext.sc.emptyRDD,ProgramContext.sc.emptyRDD)
      }
      else {
        //For regular directed pagerank, uncomment the following line
        //grp.pageRank(tol)
        //For undirected pagerank without coalescing, uncomment the following line
        //UndirectedPageRank.run(graphs(v),tol,resetProb,numIter)
        //For undirected pagerank with coalescing, uncomment the following line
        UndirectedPageRank.run(Graph(grp.vertices,grp.edges.coalesce(1,true)),tol,resetProb,numIter)
      }
    }    
    
    val newseq:Seq[Graph[Double,Double]] = graphs.map(safePagerank)
    new SnapshotGraphParallel(span, newseq, intervals)
  }
  
  def partitionBy(pst: PartitionStrategyType.Value, runs:Int):SnapshotGraphParallel[VD,ED] = {
    partitionBy(pst, runs, graphs.head.edges.partitions.size)
  }
  
  def partitionBy(pst: PartitionStrategyType.Value, runs:Int, parts:Int):SnapshotGraphParallel[VD,ED] = {  
    var numParts = if (parts > 0) parts else graphs.head.edges.partitions.size
    
    if (pst != PartitionStrategyType.None) {
      var result:SnapshotGraphParallel[VD,ED] = new SnapshotGraphParallel[VD,ED](span)
      //not changing the intervals, only the graphs at their indices
      //each partition strategy for SG needs information about the graph

      //use that strategy to partition each of the snapshots
      intervals.foreach {
        case (k,v) =>
          result.addSnapshot(k,graphs(v).partitionBy(PartitionStrategies.makeStrategy(pst,v,graphs.size,runs), numParts))
      }

      result
    } else
      this
  }

}

object SnapshotGraphParallel extends Serializable  {
  //this assumes that the data is in the dataPath directory, each time period in its own files
  //and the naming convention is nodes<time>.txt and edges<time>.txt
  //TODO: extend to be more flexible about input data such as arbitrary uniform intervals are supported
  final def loadData(dataPath: String, sc:SparkContext): SnapshotGraphParallel[String,Int] = {
    var minYear:Int = Int.MaxValue
    var maxYear:Int = 0

    new java.io.File(dataPath+"/nodes/").listFiles.map {fname => 
    	val tm:Int = fname.getName.filter(_.isDigit).toInt
	minYear = math.min(minYear,tm)
	maxYear = math.max(maxYear,tm)	
    }

    val span = Interval(minYear, maxYear)
    var years = 0
    val result: SnapshotGraphParallel[String,Int] = new SnapshotGraphParallel(span)

    for (years <- minYear to maxYear) {
      val users:RDD[(VertexId,String)] = sc.textFile(dataPath + "/nodes/nodes" + years + ".txt").map(line => line.split(",")).map{parts => 
        if (parts.size > 1 && parts.head != "") 
          (parts.head.toLong, parts(1).toString) 
        else
          (0L,"Default")
      }
      var edges:Graph[Int,Int] = null
      if ((new java.io.File(dataPath + "/edges/edges" + years + ".txt")).length > 0) {
            edges = GraphLoader.edgeListFile(sc, dataPath + "/edges/edges" + years + ".txt", true)
      } else {
            edges = Graph[Int,Int](sc.emptyRDD,sc.emptyRDD)
      }
      
      val graph = edges.outerJoinVertices(users) {
      	case (uid, deg, Some(name)) => name
      	case (uid, deg, None) => ""
      }

      result.addSnapshot(Interval(years, years), graph)
    }
    result
  }
}
