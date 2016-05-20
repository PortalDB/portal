package src.main.scala.edu.drexel.cs.dbgroup.temporalgraph.tools

import java.io.FileWriter
import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph.ProgramContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object HistogramFromParquet {

  //note: this does not remove ALL logging
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)
  ProgramContext.setContext(sc)
  val sqlContext = new SQLContext(sc)


  def main(args: Array[String]): Unit ={
//    makeHistogram("./arxivParquet", LocalDate.parse("1989-01-01"),  LocalDate.parse("2017-01-01"), "./arxivHistogram")
      makeHistogram("./dblp", LocalDate.parse("2015-01-01"),  LocalDate.parse("2017-01-01"), "./dblpHistogram")

  }

  def makeHistogram(source:String, startDate:LocalDate, endDate:LocalDate, fileName:String): Unit ={
    val add = "years"
    val dates = createDates(startDate, endDate, add)
    makeHistogramNodes(source + "/nodes.parquet", dates, fileName + "/nodes.txt")
    makeHistogramNodes(source + "/edges.parquet", dates, fileName + "/edges.txt")
  }

  def makeHistogramNodes(source:String, dates:Array[LocalDate], fileName:String): Unit ={
    val fw = new FileWriter(fileName, true)

    var df = sqlContext.read.parquet(source)
    df.registerTempTable("tempTable")
    var total : Long = 0
    for(i <- 0 to dates.length - 2) {
      var startDate = dates(i)
      var endDate = dates(i+1)
      val sqlQuery = "Select Count(*) FROM tempTable where (estart >= '" + startDate + "' AND estart < '" + endDate + "')" + " OR (eend > '" + startDate + "' AND eend <= '" + endDate + "')"
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

  def createDates(startDate:LocalDate, endDate:LocalDate, add:String): Array[LocalDate] ={
    var dates = Array(startDate)
    var nextDate = startDate.plusYears(1)
    dates = dates :+ nextDate
    while(!nextDate.equals(endDate)){
      nextDate = nextDate.plusYears(1)
      dates = dates :+ nextDate
    }
    dates
  }

}
