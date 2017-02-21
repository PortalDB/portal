package edu.drexel.cs.dbgroup.temporalgraph.evaluation

import java.time.LocalDate
import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import edu.drexel.cs.dbgroup.temporalgraph.tools.GlobalPointQueries
import edu.drexel.cs.dbgroup.temporalgraph._

object GlobalPoint {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryoserializer.buffer.max", "128m")
    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)
    val sqlContext = ProgramContext.getSession
    //sqlContext.conf.set("spark.sql.files.maxPartitionBytes", "16777216")

    println("using " + conf.get("portal.partitions.sgroup", "") + " sg group")

    sqlContext.emptyDataFrame.count

    val path = args(0)
    val minYear = args(1).toInt
    val maxYear = args(2).toInt
    val range = maxYear - minYear

    val lq = new GlobalPointQueries(path)
    val r = scala.util.Random

    val startAsMili = System.currentTimeMillis()

    for (i <- 0 to 100) {
      //pick random year from minYear to maxYear
      val year = LocalDate.of(r.nextInt(range) + minYear, r.nextInt(12), 1)
      println("materializing snapshot at " + year)
      lq.getSnapshot(year).numEdges
    }

    println("total time (millis) for 100 global point queries: " + (System.currentTimeMillis()-startAsMili))

  }

}

