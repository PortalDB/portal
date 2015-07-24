//read all files in a number range in parallel
package edu.drexel.cs.dbgroup.graphxt.util

import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.mapreduce.{Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.spark.rdd.CFTextFileRDD
import org.apache.spark.input.CFInputFormat
import edu.drexel.cs.dbgroup.graphxt.ProgramContext
import edu.drexel.cs.dbgroup.graphxt._

object MultifileLoad {

  def readNodes(path: String, min: TimeIndex, max: TimeIndex): RDD[(String, String)] = {
    val nodesPath = path + "/nodes/nodes{" + NumberRangeRegex.generateRegex(min, max) + "}.txt"
    val numParts = estimateParts(nodesPath)
    println("loading with " + numParts + " partitions")
    readTextFiles(nodesPath, numParts)
  }

  def readEdges(path: String, min: TimeIndex, max: TimeIndex): RDD[(String, String)] = {
    val edgesPath = path + "/edges/edges{" + NumberRangeRegex.generateRegex(min, max) + "}.txt"
    val numParts = estimateParts(edgesPath)
    println("loading with " + numParts + " partitions")
    readTextFiles(edgesPath, numParts)
  }

  private def readTextFiles(path: String, minPartitions: Int): RDD[(String, String)] = {
    val job = NewHadoopJob.getInstance(ProgramContext.sc.hadoopConfiguration)
    NewFileInputFormat.addInputPath(job, new Path(path))
    val updateConf = job.getConfiguration
    new CFTextFileRDD(
      ProgramContext.sc,
      classOf[CFInputFormat],
      classOf[String],
      classOf[String],
      updateConf,
      minPartitions).setName(path)
  }

  def estimateParts(path: String): Int = {
    var fs: FileSystem = null
    val conf: Configuration = new Configuration()
    if (System.getenv("HADOOP_CONF_DIR") != "") {
      conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"))
    }
    fs = FileSystem.get(conf)
    val pt: Path = new Path(path)
    val len = fs.globStatus(pt).map(_.getLen / 1000000).reduce(_+_)
    println("total length in Mbytes for path " + path + " is " + len)

    //TODO: make this more intelligent
    if (0 <= len && len <= 3)
      1
    else if (len <= 10)
      4
    else if (len <= 300)
      16
    else
      32
  }
}
