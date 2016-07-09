package edu.drexel.cs.dbgroup.temporalgraph.tools

import java.io.FileWriter
import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph.ProgramContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shishir on 5/11/2016.
  */
object VerifyUKDELIS {
  private var graphType = "SG"

  //note: this does not remove ALL logging
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  implicit def dateOrdering: Ordering[LocalDate] = Ordering.fromLessThan((a,b) => a.isBefore(b))


  var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)
  ProgramContext.setContext(sc)
  val sqlContext = new SQLContext(sc)
  val rootPath = "./ukdelis/"
  val fw = new FileWriter("ukdelisVerifynodes.txt", true)

  def main(args: Array[String]): Unit = {
    val filenames = Array("2006-05-01.txt",
    "2006-06-01.txt",
    "2006-07-01.txt",
    "2006-08-01.txt",
    "2006-09-01.txt",
    "2006-10-01.txt",
    "2006-11-01.txt",
    "2006-12-01.txt",
    "2007-01-01.txt",
    "2007-02-01.txt",
    "2007-03-01.txt",
    "2007-04-01.txt"
    )

    val dates = Array("2006-05-02",
      "2006-06-02",
      "2006-07-02",
      "2006-08-02",
      "2006-09-02",
      "2006-10-02",
      "2006-11-02",
      "2006-12-02",
      "2007-01-02",
      "2007-02-02",
      "2007-03-02",
      "2007-04-02"
       )

    for( x <- filenames.indices){
      checkText(filenames(x))
      checkParquet(dates(x))
    }

    fw.close()
  }

  def checkText(filepath:String): Unit ={
    val line = sc.textFile(rootPath + "/nodes/nodes" + filepath)
    val nodeCountText = "nodes" + filepath + "\ncount: " + line.count() + "\n"
    print(nodeCountText)
    fw.write(nodeCountText) ;
  }

  def checkParquet(date:String): Unit ={
    val ukdelisParquet = sqlContext.read.parquet(rootPath + "/nodes.parquet").cache()
    ukdelisParquet.registerTempTable("ukdelis")
    val sqlQuery = "SELECT count(*) FROM ukdelis WHERE estart < '" + date.toString + "' and eend > '" + date + "'"
    val output = sqlContext.sql(sqlQuery)
    print(date)
    val nodeCountText = "parquet count: " + output.head.toString() + "\n\n"
    print(nodeCountText)
    fw.write(nodeCountText) ;
  }
}
