package edu.drexel.cs.dbgroup.temporalgraph.evaluation

import java.time.LocalDate
import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import edu.drexel.cs.dbgroup.temporalgraph.tools.LocalQueries
import edu.drexel.cs.dbgroup.temporalgraph._

object LocalInterval {
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
    val maxYearDate = LocalDate.of(maxYear, 1, 1)
    val range = maxYear - minYear

    val lq = new LocalQueries(path)
    val r = scala.util.Random

    val startAsMili = System.currentTimeMillis()

    for (i <- 0 to 100) {
      //pick random vid from 0 to 10K
      //pick random year from minYear to maxYear and till the end
      val id = r.nextInt(10000).toLong
      val year = LocalDate.of(r.nextInt(range) + minYear, r.nextInt(12), 1)
      println("id " + id + " from " + year + ":" + lq.getNodeHistory(id, Interval(year, maxYearDate)).collect().mkString(", "))
    }

    println("total time (millis) for 100 local interval node queries: " + (System.currentTimeMillis()-startAsMili))

    val startAsMili2 = System.currentTimeMillis()

    for (i <- 0 to 100) {
      //pick 2 random vid from 0 to 10K
      //pick random year from minYear to maxYear
      val id1 = r.nextInt(10000).toLong
      val id2 = r.nextInt(10000).toLong
      val year = LocalDate.of(r.nextInt(range) + minYear, r.nextInt(12), 1)
      println("edge " + id1 + "," + id2 + " from " + year + ":" + lq.getEdgeHistory(id1, id2, Interval(year, maxYearDate)).collect().mkString(", "))
    }

    println("total time (millis) for 100 local interval edge queries: " + (System.currentTimeMillis()-startAsMili2))

  }

}

