package edu.drexel.cs.dbgroup.temporalgraph.tools

import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.sql.catalyst.util.DateTimeUtils

import edu.drexel.cs.dbgroup.temporalgraph.ProgramContext
import edu.drexel.cs.dbgroup.temporalgraph.Interval
import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader

/**
  * All queries done on TGraphs which are global structurally,
  * i.e. refer to entire graph and in specific moment in time.
  * Global interval queries are done through operations on TGraph.
*/

class GlobalPointQueries(dataset: String) {

  def getSnapshot(point: LocalDate): Graph[Any,Any] = {
    val secs = math.floor(DateTimeUtils.daysToMillis(point.toEpochDay().toInt).toDouble / 1000L).toLong

    //TODO: move this logic elsewhere
    val sg = System.getProperty("portal.partitions.sgroup", "")
    val nodePath = GraphLoader.getPaths(dataset, Interval(point, point), "nodes_s_" + sg)
    //this is a dataframe of all nodes in the snapshot group, need to filter
    val vdfs = GraphLoader.getParquet(nodePath, point)
    val nodes = if (vdfs.schema.fields.size > 2)
      vdfs.filter("estart <= " + secs + " and eend > " + secs).rdd.map(r => (r.getLong(0), r.get(3)))
    else
      ProgramContext.sc.emptyRDD[(VertexId,Any)]

    val edgePath = GraphLoader.getPaths(dataset, Interval(point, point), "edges_s_" + sg)
    val edfs = GraphLoader.getParquet(edgePath, point)
    val edges = if (edfs.schema.fields.size > 3)
      edfs.filter("estart <= " + secs + " and eend > " + secs).rdd.map(r => Edge(r.getLong(0), r.getLong(1), r.get(4)))
    else
      ProgramContext.sc.emptyRDD[Edge[Any]]

    Graph(nodes, edges)
  }

}
