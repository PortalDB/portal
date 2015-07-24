package org.apache.spark.input

import org.apache.hadoop.fs.Path
//import org.apache.hadoop.mapreduce.lib.input.LineRecordReader
import org.apache.hadoop.util.LineReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, CombineFileRecordReader}
import org.apache.hadoop.conf.{Configuration, Configurable => HConfigurable}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.hadoop.io.Text

/**
  *  A trait to implement [[org.apache.hadoop.conf.Configurable Configurable]] interface.
  */
private[spark] trait CFConfigurable extends HConfigurable {
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
 extends RecordReader[String, String] with CFConfigurable {

  private[this] var startOffset: Long = split.getOffset(index)
  private[this] val end: Long = startOffset + split.getLength(index);
  private[this] var pos: Long = startOffset

  private[this] val path = split.getPath(index)
  private[this] val fs = path.getFileSystem(
    SparkHadoopUtil.get.getConfigurationFromJobContext(context))

  private[this] val key = path.toString
  private[this] var value: String = ""

  private[this] var skipFirstLine: Boolean = false
  val fileIn = fs.open(path)
  if (startOffset != 0) {
    skipFirstLine = true
    startOffset -= 1
    fileIn.seek(startOffset)
  }

  private[this] val lreader: LineReader = new LineReader(fileIn)
  if (skipFirstLine) {
    startOffset += lreader.readLine(new Text(), 0, math.min(Integer.MAX_VALUE, end - startOffset).toInt)
  }
  pos = startOffset

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = { }

  override def close(): Unit = { 
    if (lreader != null)
      lreader.close()
  }

  override def getProgress: Float = {
    if (startOffset == end)
      0.0f
    else
      math.min(1.0f, (pos - startOffset) / (end - startOffset).toFloat)
  }

  override def getCurrentKey: String = key

  override def getCurrentValue: String = value

  override def nextKeyValue(): Boolean = {
    var tmp: Text = new Text()
    var newSize: Int = 0
    if (pos < end) {
      newSize = lreader.readLine(tmp)
      pos += newSize
      value = tmp.toString
    }
    if (newSize == 0) {
      value = ""
      false
    } else
      true
  }
}

private[spark] class ConfigurableCombineFileRecordReader[K, V](
    split: InputSplit,
    context: TaskAttemptContext,
    recordReaderClass: Class[_ <: RecordReader[K, V] with HConfigurable])
  extends CombineFileRecordReader[K, V](
    split.asInstanceOf[CombineFileSplit],
    context,
    recordReaderClass
  ) with CFConfigurable {

  override def initNextRecordReader(): Boolean = {
    val r = super.initNextRecordReader()
    if (r) {
      this.curReader.asInstanceOf[HConfigurable].setConf(getConf)
    }
    r
  }
}
