package edu.drexel.cs.dbgroup.temporalgraph.tools

import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.catalyst.util.DateTimeUtils

import edu.drexel.cs.dbgroup.temporalgraph.ProgramContext
import edu.drexel.cs.dbgroup.temporalgraph.Interval
import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader

/**
  * All queries done on TGraphs which are local structurally,
  * i.e. refer to specific entity such as node, edge, path, neighborhood.
  * Point queries refer to specific moment in time.
  * Interval queries refer to some interval from graph lifetime.
  * TODO: add queries for path and neighborhood retrieval.
*/

//NOTE: reading of the snapshot group type code is temporary throughout.
//Remove once SG method is selected.
class LocalQueries(dataset: String) {

  def getNode(id: VertexId, point: LocalDate): RDD[(VertexId, Any)] = {
    //get the configuration option for snapshot groups
    //TODO: move this logic elsewhere out of this class
    val sg = System.getProperty("portal.partitions.sgroup", "")
    val nodePath = GraphLoader.getPaths(dataset, Interval(point, point.plusDays(1)), "nodes_t_" + sg)

    //assumes start and end are stored as long values of seconds since 1970
    val secs = math.floor(DateTimeUtils.daysToMillis(point.toEpochDay().toInt).toDouble / 1000L).toLong
    val dfs = GraphLoader.getParquet(nodePath, point)
    if (dfs.schema.fields.size > 3)
      dfs.filter("vid == " + id).filter("estart <= " + secs + " and eend > " + secs).rdd.map(r => (r.getLong(0), r.get(3)))
    else if (dfs.schema.fields.size > 2)
      dfs.filter("vid == " + id).filter("estart <= " + secs + " and eend > " + secs).rdd.map(r => (r.getLong(0), null))
    else
      ProgramContext.sc.emptyRDD[(VertexId, Any)]
  }

  def getEdge(srcId: VertexId, dstId: VertexId, point: LocalDate): RDD[((VertexId, VertexId), Any)] = {
    //get the configuration option for snapshot groups
    //TODO: move this logic elsewhere out of this class
    val sg = System.getProperty("portal.partitions.sgroup", "")
    val edgePath = GraphLoader.getPaths(dataset, Interval(point, point.plusDays(1)), "edges_t_" + sg)
    //assumes start and end are stored as long values of seconds since 1970
    val secs = math.floor(DateTimeUtils.daysToMillis(point.toEpochDay().toInt).toDouble / 1000L).toLong 
    val dfs = GraphLoader.getParquet(edgePath, point)
    if (dfs.schema.fields.size > 4) 
      dfs.filter("vid1 == " + srcId + " and vid2 == " + dstId).filter("estart <= " + secs + " and eend > " + secs).rdd.map(r => ((r.getLong(0), r.getLong(1)), r.get(4))) 
    else if (dfs.schema.fields.size > 3)
      dfs.filter("vid1 == " + srcId + " and vid2 == " + dstId).filter("estart <= " + secs + " and eend > " + secs).rdd.map(r => ((r.getLong(0), r.getLong(1)), null))
    else
      ProgramContext.sc.emptyRDD[((VertexId, VertexId), Any)]
  }

  /** Local interval. */
 
  //all node tuples within interval
  def getNodeHistory(id: VertexId, intv: Interval): RDD[(VertexId, (Interval,Any))] = {
    //get the configuration option for snapshot groups
    //TODO: move this logic elsewhere out of this class
    val sg = System.getProperty("portal.partitions.sgroup", "")
    val nodePaths = GraphLoader.getPaths(dataset, intv, "nodes_t_" + sg)

    val secs1 = math.floor(DateTimeUtils.daysToMillis(intv.start.toEpochDay().toInt).toDouble / 1000L).toLong
    val secs2 = math.floor(DateTimeUtils.daysToMillis(intv.end.toEpochDay().toInt).toDouble / 1000L).toLong

    val dfs = GraphLoader.getParquet(nodePaths, intv)
    if (dfs.schema.fields.size > 3)
      dfs.filter("vid == " + id).filter("NOT (estart >= " + secs2 + " OR eend <= " + secs1 + ")").rdd.map(r => (r.getLong(0), (Interval(r.getLong(1), r.getLong(2)), r.get(3))))
    else if (dfs.schema.fields.size > 2)
      dfs.filter("vid == " + id).filter("NOT (estart >= " + secs2 + " OR eend <= " + secs1 + ")").rdd.map(r => (r.getLong(0), (Interval(r.getLong(1), r.getLong(2)), null)))
    else
      ProgramContext.sc.emptyRDD[(VertexId, (Interval, Any))]
  }

  //all edge tuples within interval
  def getEdgeHistory(srcId: VertexId, dstId: VertexId, intv: Interval): RDD[((VertexId, VertexId), (Interval, Any))] = {
    //TODO: move this logic elsewhere out of this class
    val sg = System.getProperty("portal.partitions.sgroup", "")
    val edgePaths = GraphLoader.getPaths(dataset, intv, "edges_t_" + sg)

    val secs1 = math.floor(DateTimeUtils.daysToMillis(intv.start.toEpochDay().toInt).toDouble / 1000L).toLong
    val secs2 = math.floor(DateTimeUtils.daysToMillis(intv.end.toEpochDay().toInt).toDouble / 1000L).toLong

    val dfs = GraphLoader.getParquet(edgePaths, intv)
    if (dfs.schema.fields.size > 4)
      dfs.filter("vid1 == " + srcId + " and vid2 == " + dstId).filter("NOT (estart >= " + secs2 + " OR eend <= " + secs1 + ")").rdd.map(r => ((r.getLong(0), r.getLong(1)), (Interval(r.getLong(2), r.getLong(3)), r.get(4))))
    else if (dfs.schema.fields.size > 3)
     dfs.filter("vid1 == " + srcId + " and vid2 == " + dstId).filter("NOT (estart >= " + secs2 + " OR eend <= " + secs1 + ")").rdd.map(r => ((r.getLong(0), r.getLong(1)), (Interval(r.getLong(2), r.getLong(3)), null)))
    else
      ProgramContext.sc.emptyRDD[((VertexId, VertexId), (Interval, Any))]
  }
}
