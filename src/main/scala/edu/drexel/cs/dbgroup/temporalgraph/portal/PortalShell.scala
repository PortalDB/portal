package edu.drexel.cs.dbgroup.temporalgraph.portal

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.control._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader

import scala.tools.jline.console.ConsoleReader;
import scala.util.control._;
import scala.util.Properties;

object PortalShell {
  val infoText: String = "[info]";
  val errText: String = "[error]";
  val unsupportedError: String = "unsupported command";
  
  def main(args: Array[String]) = {

    //note: this does not remove ALL logging  
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    var graphType: String = "SG"
    var data = ""
    var partitionType: PartitionStrategyType.Value = PartitionStrategyType.None
    var runWidth: Int = 2
    var query: Array[String] = Array.empty

    for (i <- 0 until args.length) {
      args(i) match {
        case "--type" =>
          graphType = args(i + 1)
          graphType match {
            case "MG" =>
              println("Running experiments with MultiGraph")
            case "SG" =>
              println("Running experiments with SnapshotGraph")
            case "SGP" =>
              println("Running experiments with parallel SnapshotGraph")
            case "MGC" =>
              println("Running experiments with columnar MultiGraph")
            case "OG" =>
              println("Running experiments with OneGraph")
            case "OGC" =>
              println("Running experiments with columnar OneGraph")
            case _ =>
              println("Invalid graph type, exiting")
              System.exit(1)
          }
        case "--data" =>
          data = args(i + 1)
        case "--strategy" =>
          partitionType = PartitionStrategyType.withName(args(i + 1))
        case "--query" =>
          query = args.drop(i + 1)
        case "--runwidth" =>
          runWidth = args(i + 1).toInt
        case _ => ()
      }
    }

    //until we have a query optimizer, this will have to do
    val grp: Int = query.indexOf("group") + 2
    if (grp > 1)
      runWidth = query(grp).toInt

    // environment specific settings for SparkConf must be passed through the command line
    // settings to pass are master, jars and other configurations
    var conf = new SparkConf().setAppName("TemporalGraph Project")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setMaster("local[2]");
    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)

    GraphLoader.setPath(data)
    GraphLoader.setGraphType(graphType)
    GraphLoader.setStrategy(partitionType)
    GraphLoader.setRunWidth(runWidth)

    val startAsMili = System.currentTimeMillis()
    //PortalParser.parse(query.mkString(" "))
    startConsole()
    val stopAsMili = System.currentTimeMillis()
    val runTime = stopAsMili - startAsMili

    println(f"Final Runtime: $runTime%dms")
    sc.stop
  }

  def startConsole() = {
    val consoleReader = new ConsoleReader();
    val loop = new Breaks;
    var commandNum: Int = 0;

    printProgramStart();

    loop.breakable {
      while (true) {
        var line = consoleReader.readLine("portal> ");

        if (checkQuit(line) == true) {
          loop.break;
        }

        while (!isLineEnd(line)) {
          line += consoleReader.readLine(">");
        }

        commandNum += 1;
        if(parseCommand(line, commandNum) == false){
          //unsupported command
          commandNum -= 1;
        }
      }
    }

    println("Exiting Portal Shell...\n")

  }

  def printProgramStart() = {
    println("\n===============================================================================");
    println("\t\t\tWelcome to the Portal Shell!");
    println("\t\t    cs.drexel.edu.dbgroup.temporalgraph")
    println("===============================================================================");

  }

  def isLineEnd(line: String): Boolean = {
    if (line.takeRight(1) == "\\") {
      return true;
    }
    return false;
  }

  def checkQuit(line: String): Boolean = {
    if (line == "q" | line == Properties.lineSeparator) {
      return true;
    }
    return false;
  }

  def parseCommand(line: String, commandNum: Integer): Boolean = {
    var args = line.split(" ");
    var commandType: String = args(0);
    var tViewName: String = "";
    var queryString: String = "";

    //    for (r <- args) {
    //      println(r)
    //    }
    printf("#%d --> %s", commandNum, line);

    if (commandType.equalsIgnoreCase("create")) {
      var isMaterialized: Boolean = false;
      tViewName = args(2);

      if (args(1).equalsIgnoreCase("materialized")) {
        isMaterialized = true;
        tViewName = args(4);
      }

      println("\ntViewName --> " + tViewName);

    } else if (commandType.equalsIgnoreCase("describe")) {
      tViewName = args(1);

    } else if (commandType.equalsIgnoreCase("show")) {

    } else {
      printf("\n%s %s\n", errText, unsupportedError);
      return false
    }

    return true;
  }
}
