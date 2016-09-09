package edu.drexel.cs.dbgroup.temporalgraph.tools

import java.sql.Date

import _root_.edu.drexel.cs.dbgroup.temporalgraph.ProgramContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by shishir on 8/8/2016.
  */
object TestFilterPushdown {
  //note: this does not remove ALL logging
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))

  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.network.timeout", "240")
  //conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", "500000")
  val sc = new SparkContext(conf)
  ProgramContext.setContext(sc)
  val sqlContext = ProgramContext.getSession
  sqlContext.conf.set("spark.sql.files.maxPartitionBytes", "16777216")
  import sqlContext.implicits._

  def main(args: Array[String]): Unit ={
    //run("./dblp/edges.parquet", "1952-01-01", "2013-01-01")
    //runDate("hdfs://master:9000/data/twitter/edges.parquet", "2006-05-01", "2012-05-01")
    //runVid("hdfs://master:9000/data/twitter/edges.parquet", 1000, 10000000)
    runVid("hdfs://master:9000/data/twitter/nodes.parquet", 100, 10000000)
  }

  def runVid(source:String, smallVid:Long, largeVid:Long): Unit ={
    val data = sqlContext.read.parquet(source)
    var start, end, timeTaken: Long = 0
    data.registerTempTable("tempTable")
    //executing test predicate to make sure the tempTable is registered
    var output = sqlContext.sql("Select * from tempTable")
    println(output.count())


    //to test if number of counts affect performance keeping number of predicate constant, it looks like it doesnt
    start = System.currentTimeMillis()
    var sqlQuery = "Select vid from tempTable where vid<" +  largeVid
    output = sqlContext.sql(sqlQuery)
    println(output.count())
    end = System.currentTimeMillis()
    timeTaken =  end - start
    println("Time taken when vid < " + largeVid + " = " + timeTaken/1000 + "s")
    
    start = System.currentTimeMillis()
    sqlQuery = "Select vid from tempTable where vid<" +  smallVid
    output = sqlContext.sql(sqlQuery)
    println(output.count())
    end = System.currentTimeMillis()
    timeTaken =  end - start
    println("Time taken when vid < " + smallVid  + " = " + timeTaken/1000 + "s")
  }


  def runDate(source:String, estartDateWithFewCounts:String, estartDateWithManyCounts:String): Unit ={
    val data = sqlContext.read.parquet(source)
    var start, end, timeTaken: Long = 0
    data.registerTempTable("tempTable")
    //executing test predicate to make sure the tempTable is registered
    var output = sqlContext.sql("Select * from tempTable")
    println(output.count())

    start = System.currentTimeMillis()
    var sqlQuery = "Select estart from tempTable where estart='" + estartDateWithFewCounts + "'"
    output = sqlContext.sql(sqlQuery)
    println(output.count())
    end = System.currentTimeMillis()
    timeTaken =  end - start
    println("Time taken when estart = " + estartDateWithFewCounts + " : " + timeTaken/1000 + "s")

    //to test if number of counts affect performance keeping number of predicate constant, it looks like it doesnt
    start = System.currentTimeMillis()
    sqlQuery = "Select estart from tempTable where estart='" + estartDateWithManyCounts + "'"
    output = sqlContext.sql(sqlQuery)
    println(output.count())
    end = System.currentTimeMillis()
    timeTaken =  end - start
    println("Time taken when estart =  " + estartDateWithManyCounts + " : " + timeTaken/1000 + "s")
  }

}
