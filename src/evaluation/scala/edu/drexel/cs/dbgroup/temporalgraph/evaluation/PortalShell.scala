package edu.drexel.cs.dbgroup.temporalgraph.evaluation

import edu.drexel.cs.dbgroup.temporalgraph.{ProgramContext, PartitionStrategyType}
import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.control._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object PortalShell {

  var uri = ""
  var warmStart: Boolean = false

  def main(args: Array[String]) = {

    //note: this does not remove ALL logging  
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("DataNucleus").setLevel(Level.OFF)

    var graphType: String = "SG"
    var partitionType: PartitionStrategyType.Value = PartitionStrategyType.None
    var runWidth: Int = 8
    var query:Array[String] = Array.empty

    for (i <- 0 until args.length) {
      args(i) match {
        case "--type" =>
          graphType = args(i + 1)
          graphType match {
            case "SG" =>
              println("Running experiments with SnapshotGraph")
            case "OG" =>
              println("Running experiments with OneGraph")
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
    //This is a workaround for spark memory leak JIRA SPARK-14560
    conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", "500000")
 
    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)
    val sqlContext = ProgramContext.getSqlContext

    GraphLoader.setGraphType(graphType)
    GraphLoader.setStrategy(partitionType)
    GraphLoader.setRunWidth(runWidth)

    val startAsMili = System.currentTimeMillis()
    PortalParser.setStrategy(partitionType)
    PortalParser.setRunWidth(runWidth)
    PortalParser.parse(query.mkString(" "))
    val stopAsMili = System.currentTimeMillis()
    val runTime = stopAsMili - startAsMili

    println(f"Final Runtime: $runTime%dms")
    sc.stop
  }
}

