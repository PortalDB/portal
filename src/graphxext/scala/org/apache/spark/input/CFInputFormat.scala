package org.apache.spark.input

import scala.collection.JavaConversions._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit

class CFInputFormat extends CombineFileInputFormat[String, String] with Configurable {

  override def createRecordReader(
    split: InputSplit,
    context: TaskAttemptContext): RecordReader[String, String] = {
    val reader =
      new ConfigurableCombineFileRecordReader(split, context, classOf[CFRecordReader])
    reader.setConf(getConf)
    reader
  }

  def setMinPartitions(context: JobContext, minPartitions: Int) {
    val files = listStatus(context)
    val totalLen = files.map { _.getLen }.sum
    val maxSplitSize = Math.ceil(totalLen * 1.0 /
      (if (minPartitions == 0) 1 else minPartitions)).toLong
    super.setMaxSplitSize(maxSplitSize)
  }

}
