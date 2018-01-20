package org.apache.spark.graphx.impl

import scala.reflect.ClassTag

import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.PartitionStrategy

//extend the partitionstrategy interface to pass full edge info
trait PartitionStrategyMoreInfo extends PartitionStrategy {
  def getPartition[ED: ClassTag](e:Edge[ED], numParts: PartitionID): PartitionID
}


