package edu.drexel.cs.dbgroup.temporalgraph.tools

import java.time.Period
import java.time.LocalDate
import java.sql.Date

import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.util.SizeEstimator
import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.{GraphSplitter, TempGraphOps}
import org.apache.spark.storage.StorageLevel

/**
  * Takes Parquet datasets of edges and saves a new one with edge ids to support multigraphs. 
*/
object AssignEdgeIds {
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
    var source: String = args(0)
    var dest: String = args(1)

    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)
    val sqlContext = ProgramContext.getSession
    //import sqlContext.implicits._
    sqlContext.conf.set("spark.sql.files.maxPartitionBytes", "16777216")
    sqlContext.conf.set("spark.sql.shuffle.partitions", "1024")
    convert(source, dest)

    sc.stop
  }

  def convert(source: String, dest: String): Unit = {
    //load the datasets
    val edges = ProgramContext.getSession.read.parquet(source)
    
    val ids = edges.select("vid1","vid2").distinct.orderBy("vid1","vid2").withColumn("eid",monotonicallyIncreasingId)
    val edgeswithid = edges.join(ids, Seq("vid1","vid2"), "inner").select("eid", edges.columns:_*)
    edgeswithid.write.parquet(dest)
  }

}

