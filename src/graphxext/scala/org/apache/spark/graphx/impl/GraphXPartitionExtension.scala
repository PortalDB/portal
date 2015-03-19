package org.apache.spark.graphx.impl

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.PartitionStrategy._

object GraphXPartitionExtension {
  //an implicit class allows us to override one method of GraphImpl without
  //having to modify every instance of new GraphImpl in the code
  implicit class GraphImplExtension[VD: ClassTag, ED: ClassTag](graph: GraphImpl[VD,ED]) {
    def partitionBy(partitionStrategy: PartitionStrategyMoreInfo, numPartitions: Int): Graph[VD, ED] = {
      if (partitionStrategy.isInstanceOf[PartitionStrategyMoreInfo]) {
        val edTag = classTag[ED]
        val vdTag = classTag[VD]
        val newEdges = graph.edges.withPartitionsRDD(graph.replicatedVertexView.edges.map { e =>
          val part: PartitionID = partitionStrategy.getPartition(e, numPartitions)
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
        GraphImpl.fromExistingRDDs(graph.vertices.withEdges(newEdges), newEdges)
      } else {
        graph.partitionBy(partitionStrategy, numPartitions)
      }
    }
  }
}
