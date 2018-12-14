package edu.drexel.cs.dbgroup.portal.evaluation

import java.time.LocalDate
import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import edu.drexel.cs.dbgroup.portal.tools.GlobalPointQueries
import edu.drexel.cs.dbgroup.portal._
import scala.io.Source

object GlobalPoint {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)
    val sqlContext = ProgramContext.getSession

    println("using " + System.getProperty("portal.partitions.sgroup", "") + " sg group")

    sqlContext.emptyDataFrame.count

    val path = args(0)
    val dates = Source.fromFile(args(1)).getLines.map(l => LocalDate.parse(l))

    val lq = new GlobalPointQueries(path)

    val startAsMili = System.currentTimeMillis()

    dates.foreach { year =>
      println("materializing snapshot at " + year)
      println("edge count: " + lq.getSnapshot(year).numEdges)
    }

    println("total time (millis) for 100 global point queries: " + (System.currentTimeMillis()-startAsMili))

  }

}

