package edu.drexel.cs.dbgroup.graphxt.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, CombineFileRecordReader}
import org.apache.hadoop.conf.{Configuration, Configurable => HConfigurable}

/**
 * A trait to implement [[org.apache.hadoop.conf.Configurable Configurable]] interface.
 */
private[graphxt] trait Configurable extends HConfigurable {
  private var conf: Configuration = _
  def setConf(c: Configuration) {
    conf = c
  }
  def getConf: Configuration = conf
}

class CFRecordReader(
    split: CombineFileSplit,
    context: TaskAttemptContext,
    index: Integer)
 extends RecordReader[String, String] with Configurable {

  private[this] val path = split.getPath(index)
  private[this] val key = path.toString
  private[this] val reader = new LineRecordReader()

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    reader.initialize(split, context)
  }

  override def close(): Unit = reader.close() 

  override def getProgress: Float = reader.getProgress()

  override def getCurrentKey: String = key

  override def getCurrentValue: String = reader.getCurrentValue().toString

  override def nextKeyValue(): Boolean = reader.nextKeyValue()

}

private[graphxt] class ConfigurableCombineFileRecordReader[K, V](
    split: InputSplit,
    context: TaskAttemptContext,
    recordReaderClass: Class[_ <: RecordReader[K, V] with HConfigurable])
  extends CombineFileRecordReader[K, V](
    split.asInstanceOf[CombineFileSplit],
    context,
    recordReaderClass
  ) with Configurable {

  override def initNextRecordReader(): Boolean = {
    val r = super.initNextRecordReader()
    if (r) {
      this.curReader.asInstanceOf[HConfigurable].setConf(getConf)
    }
    r
  }
}
