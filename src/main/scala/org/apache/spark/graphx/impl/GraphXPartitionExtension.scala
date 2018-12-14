package org.apache.spark.graphx.impl

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.PartitionStrategy._
//import org.apache.spark.graphx.GraphXPartitionExt._

object GraphXPartitionExtension {
  //an implicit class allows us to override one method of GraphImpl without
  //having to modify every instance of new GraphImpl in the code
  implicit class GraphImplExtension[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]) extends Serializable {
    def partitionByExt(partitionStrategy: PartitionStrategy, numPartitions: Int): Graph[VD, ED] = {
      partitionStrategy match {
        case ps: PartitionStrategyMoreInfo =>
          graph match {
            case g2: GraphImpl[VD,ED] =>
              val edTag = classTag[ED]
              val vdTag = classTag[VD]
              val newEdges = g2.replicatedVertexView.edges.withPartitionsRDD(g2.replicatedVertexView.edges.map { e =>
                val part: PartitionID = ps.getPartition(e, numPartitions)
                (part, (e.srcId, e.dstId, e.attr))
              }
                .partitionBy(new HashPartitioner(numPartitions))
                .mapPartitionsWithIndex( { (pid, iter) =>
                  val builder = new EdgePartitionBuilder[ED, VD]()(edTag, vdTag)
                  iter.foreach { message =>
                    val data = message._2
                    builder.add(data._1, data._2, data._3)
                  }
                  val edgePartition = builder.toEdgePartition
                  Iterator((pid, edgePartition))
                }, preservesPartitioning = true)).cache()
              GraphImpl.fromExistingRDDs(g2.vertices.withEdges(newEdges), newEdges)

            case _ => throw new ClassCastException
          }
        case oldps: PartitionStrategy =>
          graph.partitionBy(partitionStrategy, numPartitions)
        case _ => throw new ClassCastException
      }
    }
  }

}
