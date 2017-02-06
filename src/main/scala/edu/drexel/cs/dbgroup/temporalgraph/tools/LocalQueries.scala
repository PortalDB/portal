package edu.drexel.cs.dbgroup.temporalgraph.tools

import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId

import edu.drexel.cs.dbgroup.temporalgraph.ProgramContext
import edu.drexel.cs.dbgroup.temporalgraph.Interval
import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader

class LocalQueries(nodePaths: Array[String], edgePaths: Array[String]) {
  final val SECONDS_PER_DAY = 60 * 60 * 24L

  def getNode(id: VertexId, point: LocalDate): RDD[(VertexId, Any)] = {
    //given a path of files, it's fastest to ping directly
    //especially if sorted by vid

    //assumes start and end are stored as long values of seconds since 1970
    val secs = point.toEpochDay()*SECONDS_PER_DAY
    //FIXME: need error handling
    GraphLoader.getParquet(nodePaths, point).filter("vid == " + id).filter("estart <= " + secs + " and eend > " + secs).rdd.map(r => (r.getLong(0), r.get(3)))
  }

  def getEdge(srcId: VertexId, dstId: VertexId, point: LocalDate): RDD[((VertexId, VertexId), Any)] = {
    //assumes start and end are stored as long values of seconds since 1970
    val secs = point.toEpochDay()*SECONDS_PER_DAY
    GraphLoader.getParquet(edgePaths, point).filter("vid1 == " + srcId + " and vid2 == " + dstId).filter("estart <= " + secs + " and eend > " + secs).rdd.map(r => ((r.getLong(0), r.getLong(1)), r.get(4)))
  }

  /** Local interval. */
  
  def getNodeHistory(id: VertexId, intv: Interval): RDD[(VertexId, (Interval,Any))] = {
    //all node tuples within interval
    GraphLoader.getParquet(nodePaths, intv).filter("vid == " + id).filter("NOT (estart >= " + intv.end.toEpochDay()*SECONDS_PER_DAY + " OR eend <= " + intv.start.toEpochDay()*SECONDS_PER_DAY + ")").rdd.map(r => (r.getLong(0), (Interval(LocalDate.ofEpochDay(r.getLong(1) / SECONDS_PER_DAY), LocalDate.ofEpochDay(r.getLong(2) / SECONDS_PER_DAY)), r.get(3))))

  }

  def getEdgeHistory(srcId: VertexId, dstId: VertexId, intv: Interval): RDD[((VertexId, VertexId), (Interval, Any))] = {
    //all edge tuples within interval
    GraphLoader.getParquet(edgePaths, intv).filter("vid1 == " + srcId + " and vid2 == " + dstId).filter("NOT (estart >= " + intv.end.toEpochDay()*SECONDS_PER_DAY + " OR eend <= " + intv.start.toEpochDay()*SECONDS_PER_DAY + ")").rdd.map(r => ((r.getLong(0), r.getLong(1)), (Interval(LocalDate.ofEpochDay(r.getLong(2) / SECONDS_PER_DAY), LocalDate.ofEpochDay(r.getLong(3) / SECONDS_PER_DAY)), r.get(4))))
  }
}
