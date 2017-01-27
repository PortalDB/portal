package edu.drexel.cs.dbgroup.temporalgraph.evaluation

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

import edu.drexel.cs.dbgroup.temporalgraph.{ProgramContext,Interval}
import edu.drexel.cs.dbgroup.temporalgraph.util.{TemporalAlgebra,TimePartitioner}

object Partitioning extends Enumeration {
  val None, TimeSplit = Value
}

object Splitters {

  def main(args: Array[String]): Unit = {

    //note: this does not remove ALL logging  
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("DataNucleus").setLevel(Level.OFF)
    // settings to pass are master, jars and other configurations
    var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.network.timeout", "240")   
 
    var uri: String = ""
    var quant: Int = 0
    var ptype = Partitioning.None
    var attName: String = ""

    for (i <- 0 until args.length) {
      args(i) match {
        case "--type" =>
          ptype = Partitioning.withName(args(i + 1))
        case "--data" =>
          uri = args(i + 1)
        case "--size" =>
          quant = args(i + 1).toInt
        case "--field" =>
          attName = args(i + 1)
        case _ => ()
      }
    }

    if (attName == "") {
      println("need valid field name, exiting")
      return
    }

    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)
    val sqlContext = ProgramContext.getSession
    import sqlContext.implicits._
    //TODO: remove hard-coding of this parameter. currently it is 1024x1024x16, i.e. 16mb
    sqlContext.conf.set("spark.sql.files.maxPartitionBytes", "16777216")
    //force workers to load first
    sqlContext.emptyDataFrame.count

    val startAsMili = System.currentTimeMillis()

    //something simple, like number of tuples for each first letter of attribute
    val rdd: RDD[(String, Interval)] = ProgramContext.getSession.read.parquet(uri).select($"estart", $"eend", col(attName)).limit(quant).rdd.map(row => (row.getString(2), Interval(row.getDate(0).toLocalDate(), row.getDate(1).toLocalDate())))

    val partitioned = ptype match {
      case Partitioning.None => Seq(rdd)
      case Partitioning.TimeSplit => TimePartitioner.partition(rdd)
    }

    //println("number of partitions: " + partitioned.getNumPartitions)
    //FIXME: we need to do the operation independently in each partition
    //otherwise things get added up extra
    val res = partitioned.map(data => TemporalAlgebra.aggregate(data, groupBy = (st: String) => st.head, seqFunc = (st: String, c: Int) => c+1, aggFunc = (a: Int, b: Int) => a+b, zeroVal = 0)).reduce(_ union _)
    println("results size: " + res.count)

    val stopAsMili = System.currentTimeMillis()
    val runTime = stopAsMili - startAsMili

    println(f"Final Runtime: $runTime%dms")

    println("results:\n" + res.sortBy(_._1).collect().mkString("\n"))
    sc.stop
  }
}
