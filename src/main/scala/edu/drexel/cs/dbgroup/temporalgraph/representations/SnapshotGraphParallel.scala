package edu.drexel.cs.dbgroup.temporalgraph.representations

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

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.{MultifileLoad,TempGraphOps,NumberRangeRegex}
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps._

import java.time.LocalDate

class SnapshotGraphParallel[VD: ClassTag, ED: ClassTag](verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[((VertexId, VertexId), (Interval, ED))], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY) extends TGraphNoSchema[VD, ED](verts, edgs) with Serializable {

  val storageLevel = storLevel
  val defaultValue: VD = defValue
  //TODO: we should enforce the integrity constraint
  //by removing edges which connect nonexisting vertices at some time t
  //or throw an exception upon construction

  //compute the graphs. due to spark lazy evaluation,
  //if these graphs are not needed, they aren't actually materialized
  val graphs: ParSeq[Graph[VD,ED]] = intervals.map( p =>
    Graph(verts.filter(v => v._2._1.intersects(p)).map(v => (v._1, v._2._2)), 
      edgs.filter(e => e._2._1.intersects(p)).map(e => Edge(e._1._1, e._1._2, e._2._2)),
      defaultValue, storageLevel, storageLevel)).par

  override def materialize() = {
    graphs.foreach { x =>
      if (!x.edges.isEmpty)
        x.numEdges
      if (!x.vertices.isEmpty)
        x.numVertices
    }
  }

  override def getSnapshot(time: LocalDate): Graph[VD,ED] = {
    val index = intervals.indexWhere(a => a.contains(time))
    if (index >= 0) {
      graphs(index)
    } else
      Graph[VD,ED](ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD)
  }

  /** Query operations */

  override def slice(bound: Interval): SnapshotGraphParallel[VD, ED] = {
    if (span.start.isEqual(bound.start) && span.end.isEqual(bound.end)) return this

    if (!span.intersects(bound)) {
      return SnapshotGraphParallel.emptyGraph[VD,ED](defaultValue)
    }

    val startBound = if (bound.start.isAfter(span.start)) bound.start else span.start
    val endBound = if (bound.end.isBefore(span.end)) bound.end else span.end
    val selectBound:Interval = Interval(startBound, endBound)

    new SnapshotGraphParallel(allVertices.filter{ case (vid, (intv, attr)) => intv.intersects(selectBound)}.mapValues(y => (Interval(maxDate(y._1.start, startBound), minDate(y._1.end, endBound)), y._2)), allEdges.filter{ case (vids, (intv, attr)) => intv.intersects(selectBound)}.mapValues(y => (Interval(maxDate(y._1.start, startBound), minDate(y._1.end, endBound)), y._2)), defaultValue, storageLevel)
  }

  override def select(vtpred: Interval => Boolean, etpred: Interval => Boolean): SnapshotGraphParallel[VD, ED] = {
    //FIXME: what if anything should be done with the period values of
    //remaining vertices?

    //because of the integrity constraint on edges, they have to 
    //satisfy both predicates
    new SnapshotGraphParallel(allVertices.filter{ case (vid, (intv, attr)) => vtpred(intv)}, allEdges.filter{ case (ids, (intv, attr)) => vtpred(intv) && etpred(intv)}, defaultValue, storageLevel)

  }

  override def select(epred: ((VertexId, VertexId), (Interval, ED)) => Boolean = (ids, attrs) => true, vpred: (VertexId, (Interval, VD)) => Boolean = (vid, attrs) => true): SnapshotGraphParallel[VD, ED] = {
    //TODO: if the vpred is not provided, i.e. is true
    //then we can skip most of the work on enforcing integrity constraints with V

    //can do this in two ways:
    //1. subgraph each representative graph, which takes care of integrity constraint on E, then pull new vertex and edge rdds, and coalesce
    //2. simple select on vertices, then join the coalesced by structure result
    //to modify edges

    //this is method 2
    val newVerts: RDD[(VertexId, (Interval, VD))] = allVertices.filter{ case (vid, attrs) => vpred(vid, attrs)}
    val coalescV: RDD[(VertexId, Interval)] = coalesceStructure(newVerts)
    val filteredEdges: RDD[((VertexId, VertexId), (Interval, ED))] = allEdges.filter{ case (ids, attrs) => epred(ids, attrs)}
    //get edges that are valid for each of their two vertices
    val e1: RDD[((VertexId, VertexId), (Interval, ED))] = filteredEdges.map(e => (e._1._1, e))
      .join(coalescV) //this creates RDD[(VertexId, (((VertexId, VertexId), (Interval, ED)), Interval))]
      .filter{case (vid, (e, v)) => e._2._1.intersects(v) } //this keeps only matches of vertices and edges where periods overlap
      .map{case (vid, (e, v)) => (e._1, (Interval(maxDate(e._2._1.start, v.start), minDate(e._2._1.end, v.end)), e._2._2))} //because the periods overlap we don't have to worry that maxdate is after mindate
    val e2: RDD[((VertexId, VertexId), (Interval, ED))] = filteredEdges.map(e => (e._1._2, e))
      .join(coalescV) //this creates RDD[(VertexId, (((VertexId, VertexId), (Interval, ED)), Interval))]
      .filter{case (vid, (e, v)) => e._2._1.intersects(v) } //this keeps only matches of vertices and edges where periods overlap
      .map{case (vid, (e, v)) => (e._1, (Interval(maxDate(e._2._1.start, v.start), minDate(e._2._1.end, v.end)), e._2._2))} //because the periods overlap we don't have to worry that maxdate is after mindate
    //now join them to keep only those that satisfy both foreign key constraints
    val newEdges: RDD[((VertexId, VertexId), (Interval, ED))] = e1.join(e2)
    //keep only an edge that meets constraints on both vertex ids
      .filter{ case (k, (e1, e2)) => e1._1.intersects(e2._1) && e1._2 == e2._2 }
      .map{ case (k, (e1, e2)) => (k, (Interval(maxDate(e1._1.start, e2._1.start), minDate(e1._1.end, e2._1.end)), e1._2))}
    //no need to coalesce either vertices or edges because we are removing some entities, but not extending them or modifying attributes

    new SnapshotGraphParallel(newVerts, newEdges, defaultValue, storageLevel)

  }

  def aggregate(res: Resolution, vgroupby: (VertexId, VD) => VertexId, egroupby: EdgeTriplet[VD, ED] => (VertexId, VertexId), vquant: AggregateSemantics.Value, equant: AggregateSemantics.Value, vAggFunc: (VD, VD) => VD, eAggFunc: (ED, ED) => ED): TGraph[VD, ED] = {
    //TODO!!
//    if (allVertices.isEmpty)
    return SnapshotGraphParallel.emptyGraph[VD,ED](defaultValue)

  }

  override def project[ED2: ClassTag, VD2: ClassTag](emap: (Edge[ED], Interval) => ED2, vmap: (VertexId, Interval, VD) => VD2): SnapshotGraphParallel[VD2, ED2] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def mapVertices[VD2: ClassTag](map: (VertexId, Interval, VD) => VD2)(implicit eq: VD =:= VD2 = null): SnapshotGraphParallel[VD2, ED] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def mapEdges[ED2: ClassTag](map: (Edge[ED], Interval) => ED2): SnapshotGraphParallel[VD, ED2] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, Map[Interval, U])])(mapFunc: (VertexId, Interval, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): SnapshotGraphParallel[VD2, ED] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def union(other: TGraph[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): SnapshotGraphParallel[VD, ED] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def intersection(other: TGraph[VD, ED], vFunc: (VD, VD) => VD, eFunc: (ED, ED) => ED): SnapshotGraphParallel[VD, ED] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def pregel[A: ClassTag]
     (initialMsg: A, defValue: A, maxIterations: Int = Int.MaxValue,
       activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
       sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
       mergeMsg: (A, A) => A): SnapshotGraphParallel[VD, ED] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def degree: RDD[(VertexId, Map[Interval, Int])] = {
    if (!allEdges.isEmpty) {
      val total = graphs.zipWithIndex
        .map(x => (x._1, intervals(x._2)))
        .filterNot(x => x._1.edges.isEmpty)
        .map(x => x._1.degrees.mapValues(deg => Map[Interval, Int](x._2 -> deg)))
      if (total.size > 0)
        total.reduce((x,y) => VertexRDD(x union y))
          .reduceByKey((a: Map[Interval, Int], b: Map[Interval, Int]) => a ++ b)
      else {
        ProgramContext.sc.emptyRDD
      }
    } else {
      ProgramContext.sc.emptyRDD
    }
  }

  //run PageRank on each contained snapshot
  override def pageRank(uni: Boolean, tol: Double, resetProb: Double = 0.15, numIter: Int = Int.MaxValue): RDD[(VertexId, Map[Interval, Double])] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def connectedComponents(): RDD[(VertexId, Map[Interval, VertexId])] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  override def shortestPaths(landmarks: Seq[VertexId]): RDD[(VertexId, Map[Interval, Map[VertexId, Int]])] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

  /** Spark-specific */

  override def numPartitions(): Int = {
    graphs.filterNot(_.edges.isEmpty).map(_.edges.partitions.size).reduce(_ + _)
  }

  override def persist(newLevel: StorageLevel = MEMORY_ONLY): SnapshotGraphParallel[VD, ED] = {
    super.persist(newLevel)
    //persist each graph
    //this will throw an exception if the graphs are already persisted
    //with a different storage level
    graphs.map(g => g.persist(newLevel))
    this
  }

  override def unpersist(blocking: Boolean = true): SnapshotGraphParallel[VD, ED] = {
    super.unpersist(blocking)
    graphs.map(_.unpersist(blocking))
    this
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int): SnapshotGraphParallel[VD, ED] = {
    partitionBy(pst, runs, 0)
  }

  override def partitionBy(pst: PartitionStrategyType.Value, runs: Int, parts: Int): SnapshotGraphParallel[VD, ED] = {
    throw new UnsupportedOperationException("not yet implemented")
  }

}

object SnapshotGraphParallel extends Serializable {
/*
  //this assumes that the data is in the dataPath directory, each time period in its own files
  //end is not inclusive, i.e. [start, end)
  final def loadData(dataPath: String, start:LocalDate, end:LocalDate): SnapshotGraphParallel[String, Int] = {
    loadWithPartition(dataPath, start, end, PartitionStrategyType.None, 2)
  }

  final def loadWithPartition(dataPath: String, start: LocalDate, end: LocalDate, strategy: PartitionStrategyType.Value, runWidth: Int): SnapshotGraphParallel[String, Int] = {
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

    var intvs: Seq[Interval] = Seq[Interval]()
    var gps: ParSeq[Graph[String, Int]] = ParSeq[Graph[String, Int]]()
    var xx:LocalDate = minDate

    val total = res.numBetween(minDate, maxDate)

    while (xx.isBefore(maxDate)) {
      val nodesPath = dataPath + "/nodes/nodes" + xx.toString() + ".txt"
      val edgesPath = dataPath + "/edges/edges" + xx.toString() + ".txt"
      val numNodeParts = MultifileLoad.estimateParts(nodesPath) 
      val numEdgeParts = MultifileLoad.estimateParts(edgesPath) 
      
      val users: RDD[(VertexId, String)] = ProgramContext.sc.textFile(nodesPath, numNodeParts).map(line => line.split(",")).map { parts =>
        if (parts.size > 1 && parts.head != "")
          (parts.head.toLong, parts(1).toString)
        else
          (0L, "Default")
      }
      
      var edges: EdgeRDD[Int] = EdgeRDD.fromEdges(ProgramContext.sc.emptyRDD)

      val ept: Path = new Path(edgesPath)
      if (fs.exists(ept) && fs.getFileStatus(ept).getLen > 0) {
        //uses extended version of Graph Loader to load edges with attributes
        var g = GraphLoaderAddon.edgeListFile(ProgramContext.sc, edgesPath, true, numEdgeParts)
        if (strategy != PartitionStrategyType.None) {
          g = g.partitionBy(PartitionStrategies.makeStrategy(strategy, intvs.size + 1, total, runWidth), g.edges.partitions.size)
        }
        edges = g.edges
      }
      
      intvs = intvs :+ res.getInterval(xx)
      //TODO: should decide the storage level based on size
      //for small graphs MEMORY_ONLY is fine
      gps = gps :+ Graph(users, edges, "Default", edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)
      xx = intvs.last.end

      /*
       val degs: RDD[Double] = gps.last.degrees.map{ case (vid,attr) => attr}
       println("min degree: " + degs.min)
       println("max degree: " + degs.max)
       println("average degree: " + degs.mean)
       val counts = degs.histogram(Array(0.0, 10, 50, 100, 1000, 5000, 10000, 50000, 100000, 250000))
       println("histogram:" + counts.mkString(","))
       println("number of vertices: " + gps.last.vertices.count)
       println("number of edges: " + gps.last.edges.count)
       println("number of partitions in edges: " + gps.last.edges.partitions.size)
      */

    }

    new SnapshotGraphParallel(intvs, gps)
  }

 */
  def emptyGraph[VD: ClassTag, ED: ClassTag](defVal: VD):SnapshotGraphParallel[VD, ED] = new SnapshotGraphParallel(ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, defVal)
 
}
