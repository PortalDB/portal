package edu.drexel.cs.dbgroup.portal.evaluation

import edu.drexel.cs.dbgroup.portal.{PartitionStrategyType, ProgramContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object PortalShell {

  var uri = ""
  var warmStart: Boolean = false

  def main(args: Array[String]) = {

    //note: this does not remove ALL logging  
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("DataNucleus").setLevel(Level.OFF)

    var graphType: String = "RG"
    var partitionType: PartitionStrategyType.Value = PartitionStrategyType.None
    var runWidth: Int = 8
    var query:Array[String] = Array.empty

    for (i <- 0 until args.length) {
      args(i) match {
        case "--type" =>
          graphType = args(i + 1)
          graphType match {
            case "RG" =>
              println("Running experiments with SnapshotGraph")
            case "OG" =>
              println("Running experiments with OneGraph")
            case "OGC" =>
              println("Running experiments with OneGraphColumn")
            case "HG" =>
              println("Running experiments with HybridGraph")
            case "VE" =>
              println("Running experiments with VEGraph")
            case _ =>
              println("Invalid graph type, exiting")
              System.exit(1)
          }
        case "--data" =>
          uri = args(i + 1)
        case "--strategy" =>
          partitionType = PartitionStrategyType.withName(args(i + 1))
        case "--query" =>
          query = args.drop(i+1)
        case "--runWidth" =>
          runWidth = args(i + 1).toInt
        case "--warmStart" =>
          warmStart = true
        case _ => ()
      }
    }

    //until we have a query optimizer, this will have to do
    val grp:Int = query.indexOf("group") + 2
    if (grp > 1)
      runWidth = query(grp).toInt

    // environment specific settings for SparkConf must be passed through the command line
    // settings to pass are master, jars and other configurations
    var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.network.timeout", "240")   
 
    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)
    val sqlContext = ProgramContext.getSession
    //TODO: remove hard-coding of this parameter. currently it is 1024x1024x16, i.e. 16mb
    sqlContext.conf.set("spark.sql.files.maxPartitionBytes", "16777216")

    //force workers to load first
    sqlContext.emptyDataFrame.count
    //println("starting the timer")

    val startAsMili = System.currentTimeMillis()
    PortalParser.setStrategy(partitionType)
    PortalParser.setRunWidth(runWidth)
    //TODO - move this into query
    PortalParser.setGraphType(graphType)
    PortalParser.parse(query.mkString(" "))
    val stopAsMili = System.currentTimeMillis()
    val runTime = stopAsMili - startAsMili

    println(f"Final Runtime: $runTime%dms")
    sc.stop
  }
}

