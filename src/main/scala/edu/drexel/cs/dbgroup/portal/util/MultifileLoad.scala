//read all files in a number range in parallel
package edu.drexel.cs.dbgroup.portal.util

import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.mapreduce.{Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.spark.rdd.CFTextFileRDD
import org.apache.spark.input.CFInputFormat
import edu.drexel.cs.dbgroup.portal._
import java.time.LocalDate

object MultifileLoad {

  /** this is in the inclusive-inclusive model */
  def readNodes(path: String, min: LocalDate, max: LocalDate): RDD[(String, String)] = {
    val nodesPath = path + "/nodes/nodes{" + NumberRangeRegex.generateRegex(min.getYear(), max.getYear()) + "}-{0" + NumberRangeRegex.generateRegex(min.getMonthValue(), max.getMonthValue()) + "}-01.txt"

    val numParts = estimateParts(nodesPath)
    //println("loading with " + numParts + " partitions")
    readTextFiles(nodesPath, min, max, numParts)
  }

  def readEdges(path: String, min: LocalDate, max: LocalDate): RDD[(String, String)] = {
    val edgesPath = path + "/edges/edges{" + NumberRangeRegex.generateRegex(min.getYear(), max.getYear()) + "}-{0" + NumberRangeRegex.generateRegex(min.getMonthValue(), max.getMonthValue()) + "}-01.txt"

    val numParts = estimateParts(edgesPath)
    //println("loading with " + numParts + " partitions")
    readTextFiles(edgesPath, min, max, numParts)
  }

  private def readTextFiles(path: String, min: LocalDate, max: LocalDate, minPartitions: Int): RDD[(String, String)] = {
    val job = NewHadoopJob.getInstance(ProgramContext.sc.hadoopConfiguration)
    NewFileInputFormat.setInputPaths(job, path)

    val updateConf = job.getConfiguration
    new CFTextFileRDD(
      ProgramContext.sc,
      classOf[CFInputFormat],
      classOf[String],
      classOf[String],
      updateConf,
      minPartitions).setName(path + min.toString)
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

    if (0 <= len && len < 8)
      2
    else if (len <= 150)
      16
    else if (len <= 1000)
      (len * 0.155 + 70).toInt
    else
      (len * 0.0488 + 150).toInt
  }
}
