package edu.drexel.cs.dbgroup.temporalgraph

import org.apache.spark.graphx.{Edge, EdgeDirection, VertexId}
import org.apache.spark.util.collection.SortDataFormat

import scala.reflect.ClassTag

/**
  * Created by mtg5014 on 2/23/2017.
  * basics copied from org.apache.spark.graphx.Edge.scala
  */
case class TEdge[ED](
    var eId: EdgeId = 0,
    var srcId: VertexId = 0,
    var dstId: VertexId = 0,
    var interval: Interval = Interval.empty,
    var attr: ED = null.asInstanceOf[ED])
  extends Serializable {
  /**
    * return a graphx Edge with the Interval and EdgeId in the attribute
    */
  def toEdge(): Edge[(EdgeId,(Interval,ED))] = {
    Edge[(EdgeId,(Interval,ED))](srcId,dstId,(eId,(interval,attr)))
  }

  def toPaired(): ((EdgeId,VertexId,VertexId),(Interval,ED)) = {
    ((eId,srcId,dstId),(interval,attr))
  }
}

object TEdge {
  /**
    * return a TEdge from a graphx Edge with an EdgeId and Interval
    * @param edge
    * @tparam ED
    * @return
    */
  def apply[ED](edge: Edge[(EdgeId,(Interval,ED))]): TEdge[ED] = {
    TEdge[ED](edge.attr._1,edge.srcId,edge.dstId,edge.attr._2._1,edge.attr._2._2)
  }

  /**
    * take a paired representation and make it a TEdge
    * @param k
    * @param v
    * @tparam ED
    * @return
    */
  def apply[ED](k: (EdgeId,VertexId,VertexId), v: (Interval,ED)): TEdge[ED] = {
    TEdge[ED](k._1,k._2,k._3,v._1,v._2)
  }
}
