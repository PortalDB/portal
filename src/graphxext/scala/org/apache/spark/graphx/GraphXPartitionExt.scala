package org.apache.spark.graphx

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object GraphXPartitionExt {
  implicit class GraphExtension[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]) {
    def partitionByExt(partitionStrategy: PartitionStrategy, numPartitions: Int): Graph[VD, ED] = {
      println("AAAAAAAAAAAAAAAAA")
      null
    }
  }

}
