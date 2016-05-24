package edu.drexel.cs.dbgroup.temporalgraph.tools

import java.io.FileWriter
import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph.ProgramContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object HistogramFromParquet{

  //note: this does not remove ALL logging
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)
  ProgramContext.setContext(sc)
  val sqlContext = new SQLContext(sc)


  def main(args: Array[String]): Unit ={
    makeHistogram("./arxivParquet", LocalDate.parse("1989-01-01"),  LocalDate.parse("2016-01-01"), 5, "years", "./arxivHistogram")
    makeHistogram("hdfs://master:9000/data/dblp", LocalDate.parse("1936-01-01"),  LocalDate.parse("2015-01-01"), 1, "years", "./dblpHistogram")
    makeHistogram("hdfs://master:9000/data/nGrams", LocalDate.parse("1520-01-01"),  LocalDate.parse("2008-01-01"), 10, "years", "./ngramsHistogram")
    makeHistogram("hdfs://master:9000/data/ukdelis", LocalDate.parse("2006-05-01"),  LocalDate.parse("2007-04-01"), 1, "months", "./ukdelisHistogram")
    makeHistogramNGramsEDGES("hdfs://master:9000/data/nGrams", createDatesArrayByYear(LocalDate.parse("1520-01-01"),  LocalDate.parse("2008-01-01"), 1), "./ngramsHistogram/edges.txt")
  }



  def makeHistogram(source:String, startDate:LocalDate, endDate:LocalDate, interval: Int, intervalType:String, directoryName:String): Unit ={
    var dates:Array[LocalDate] = Array()
    if(intervalType == "years"){
      dates = createDatesArrayByYear(startDate, endDate, interval)
    }
    else if(intervalType == "months"){
      dates = createDatesArrayByMonth(startDate, endDate, interval)
    }
    else{
      println("[Error] Please specify 'years' or 'months' when using makeHistogram Method")
      return
    }
    dates.foreach(println)
    makeHistogramNodes(source + "/nodes.parquet", dates, directoryName + "/nodes" + interval + intervalType + ".txt")
    makeHistogramNodes(source + "/edges.parquet", dates, directoryName + "/edges" + interval + intervalType + ".txt")
  }

  def makeHistogramNodes(source:String, dates:Array[LocalDate], fileName:String): Unit ={
    val fw = new FileWriter(fileName, true)

    var df = sqlContext.read.parquet(source)
    df.registerTempTable("tempTable")
    var total : Long = 0
    for(i <- 0 to dates.length - 2) {
      var startDate = dates(i)
      var endDate = dates(i+1)
//      val sqlQuery = "Select Count(*) FROM tempTable where (estart >= '" + startDate + "' AND estart < '" +
//        endDate + "')" + " OR (eend > '" + startDate + "' AND eend <= '" + endDate + "')" +
//      " OR (estart < '" + startDate + "' AND eend > '" + endDate + "')"

      val sqlQuery = "Select Count(*) FROM tempTable where NOT (estart >= '" + endDate + "' OR eend <= '" +
        startDate + "')"


      val output = sqlContext.sql(sqlQuery)
      val count = output.head.toSeq(0).asInstanceOf[Long]
      total = total + count
      println("[" + startDate +  "-" + endDate + ") " + count.toString + "\n")
      fw.write("[" + startDate +  "-" + endDate + ") " + count.toString + "\n") ;
    }
    println("initial total count of the data: " + df.count)
    println("sum of count from each year: " + total)

    fw.write("\ninitial total count of the data: " + df.count +"\n")
    fw.write("sum of count from each year: " + total +"\n")

    fw.close()
  }

  def findMinMax(source:String): Unit ={
    var df = sqlContext.read.parquet(source)
    df.registerTempTable("tempTable")
    val output = sqlContext.sql("SELECT MIN(estart), Max(eend) FROM tempTable")
    output.show
  }

  def createDatesArrayByYear(startDate:LocalDate, endDate:LocalDate, interval:Int): Array[LocalDate] ={
    var dates = Array(startDate)
    var nextDate = startDate.plusYears(interval)
    dates = dates :+ nextDate
    while(!nextDate.isAfter(endDate)){
      nextDate = nextDate.plusYears(interval)
      dates = dates :+ nextDate
    }
    dates
  }

  def createDatesArrayByMonth(startDate:LocalDate, endDate:LocalDate,  interval:Int): Array[LocalDate] ={
    var dates = Array(startDate)
    var nextDate = startDate.plusMonths(interval)
    dates = dates :+ nextDate
    while(!nextDate.isAfter(endDate)){
      nextDate = nextDate.plusMonths(interval)
      dates = dates :+ nextDate
    }
    dates
  }


  def makeHistogramNGramsEDGES(source:String, dates:Array[LocalDate], fileName:String): Unit ={
    val fw = new FileWriter(fileName, true)

    var total : Long = 0
    for(i <- 0 to dates.length - 2) {
      var startDate = dates(i)
      var endDate = dates(i+1)

      val count = sc.textFile("hdfs://master:9000/data/nGrams" + "/edges/edges" + startDate.toString + ".txt").count
      total = total + count
      println("[" + startDate +  "-" + endDate + ") " + count.toString + "\n")
      fw.write("[" + startDate +  "-" + endDate + ") " + count.toString + "\n") ;
    }
    println("initial total count of the data: (N/A) used text files to count for each year")
    println("sum of count from each year: " + total)

    fw.write("\ninitial total count of the data: (N/A) used text files to count for each year\n")
    fw.write("sum of count from each year: " + total +"\n")

    fw.close()
  }

}
