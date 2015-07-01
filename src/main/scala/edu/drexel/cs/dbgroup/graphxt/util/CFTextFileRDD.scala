package edu.drexel.cs.dbgroup.graphxt.util

import org.apache.spark.SparkContext
import org.apache.spark.SerializableWritable
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.Partition
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.conf.Configuration

private[graphxt] class NewHadoopPartition(
    rddId: Int,
    val index: Int,
    @transient rawSplit: InputSplit with Writable)
  extends Partition {

  val serializableHadoopSplit = new SerializableWritable(rawSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + index
}

class CFTextFileRDD(
    sc : SparkContext,
    inputFormatClass: Class[_ <: CFInputFormat],
    keyClass: Class[String],
    valueClass: Class[String],
    @transient conf: Configuration,
    minPartitions: Int)
  extends NewHadoopRDD[String, String](sc, inputFormatClass, keyClass, valueClass, conf) {

  override def getPartitions: Array[Partition] = {
    val inputFormat = inputFormatClass.newInstance
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }
    val jobContext = newJobContext(conf, jobId)
    inputFormat.setMinPartitions(jobContext, minPartitions)
    val rawSplits = inputFormat.getSplits(jobContext).toArray
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }
}
