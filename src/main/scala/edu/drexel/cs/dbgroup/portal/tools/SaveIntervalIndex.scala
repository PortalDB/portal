package edu.drexel.cs.dbgroup.portal.tools

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.time.LocalDate
import org.apache.hadoop.fs._
import org.apache.spark.sql.functions._
import edu.drexel.cs.dbgroup.portal._
import edu.drexel.cs.dbgroup.portal.util.GraphLoader

object SaveIntervalIndex {
  final val blocksize = 1024 * 1024 * 512.0

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("DataNucleus").setLevel(Level.OFF)
    // settings to pass are master, jars and other configurations
    var conf = new SparkConf().setAppName("Dataset Converter").setSparkHome(System.getenv("SPARK_HOME"))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.network.timeout", "240")   
    conf.set("spark.hadoop.dfs.replication", "1")

    //directory where we assume nodes.parquet and edges.parquet live
    var source: String = args.head

    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)
    val sqlContext = ProgramContext.getSession
    //import sqlContext.implicits._
    sqlContext.conf.set("spark.sql.files.maxPartitionBytes", "16777216")

    //read the nodes and edges
    val nodesFiles = GraphLoader.getPaths(source, Interval(LocalDate.MIN, LocalDate.MAX), "nodes" + "_t_")
    val edgesFiles = GraphLoader.getPaths(source, Interval(LocalDate.MIN, LocalDate.MAX), "edges" + "_t_")
    val nodes = ProgramContext.getSession.read.parquet(nodesFiles:_*).select("estart", "eend")
    val edges = ProgramContext.getSession.read.parquet(edgesFiles:_*).select("estart", "eend")

    //compute intervals which is really just a sorted list of dates
    //val dates: RDD[LocalDate] = nodes.rdd.flatMap(r => Seq(r.getLong(0), r.getLong(1))).union(edges.rdd.flatMap(r => Seq(r.getLong(0), r.getLong(1)))).distinct.sortBy(c => c)

    val dates = nodes.select("estart").withColumnRenamed("estart","dt").union(nodes.select("eend").withColumnRenamed("eend","dt")).union(edges.select("estart").withColumnRenamed("estart","dt")).union(edges.select("eend").withColumnRenamed("eend","dt")).distinct.orderBy(col("dt"))

    dates.show
    println(dates.count)

    //now save
    val pth = source + "/intervals"
    dates.coalesce(1).write.parquet(pth)
    ProgramContext.getSession.read.parquet(pth).show(200)

    sc.stop
    
  }
}
