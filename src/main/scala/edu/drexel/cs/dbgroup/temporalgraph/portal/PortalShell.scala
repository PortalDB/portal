package edu.drexel.cs.dbgroup.temporalgraph.portal

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.control._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import edu.drexel.cs.dbgroup.temporalgraph._;
import edu.drexel.cs.dbgroup.temporalgraph.portal.command._;
import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader;

import scala.collection.mutable.ListBuffer
import scala.tools.jline.console.ConsoleReader;
import scala.util.matching.Regex
import scala.util.control._;
import scala.util.Properties;

object PortalShell {
  val TViewCreationSuccess: String = "TView \'%s\' created.";
  val TViewCreationFailed: String = "Unable to create TView \'%s\'.\n%s";
  val TViewExists: String = "TView \'%s\' already exists, replacing it.\n";

  var commandList: Map[String, PortalCommand] = Map(); //tViewName -> PortalCommand
  var portalContext: PortalContext = null;

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
    portalContext = new PortalContext(sc);

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

        line = line.dropRight(1); //remove terminating "\"
        commandNum += 1;
        if (parseCommand(line, commandNum) == false) {
          //unsupported command
          commandNum -= 1;
        }
      }
    }

    println("Exiting Portal Shell...\n")
  }

  def parseCommand(line: String, commandNum: Integer): Boolean = {
    var command: PortalCommand = null;
    var args = line.split(" ");
    var commandType: String = args(0);
    var tViewName: String = null;
    var portalQuery: String = null;
    var isMaterialized: Boolean = false;
    var showType: String = null;
    var isNoop: Boolean = false;

    if (commandType.equalsIgnoreCase("create")) {
      tViewName = args(2);
      portalQuery = retrieveQuery(line);
      //printf("portalQuery retrieved from command: %s\n", portalQuery);

      if (args(1).equalsIgnoreCase("materialized")) {
        isMaterialized = true;
        tViewName = args(3);
      }

      try {
        command = new CreateViewCommand(portalContext, commandNum, portalQuery, tViewName, isMaterialized);
        command.execute();

        if (commandList.contains(tViewName)) {
          //FIXME: what is the correct action if tViewName already exists
          return printErrAndReturn(String.format(TViewExists, tViewName))
        }

        //add new command to list of commands
        commandList += (tViewName -> command)
        printInfoMessage(String.format(TViewCreationSuccess, tViewName))

      } catch {
        case ex: Exception => {
          return printErrAndReturn(String.format(TViewCreationFailed, tViewName, ex.getMessage()))
        }
      }

    } else if (commandType.equalsIgnoreCase("describe")) {

      if (args.length < 2) {
        return printErrAndReturn(PortalShellConstants.InvalidCommandFormat(commandType))
      }

      tViewName = args(1);
      var cmd = retrieveCommand(tViewName).asInstanceOf[CreateViewCommand];
      if (cmd == null) return false;

      val description = cmd.describeSchema();
      printInfoMessage(description);

    } else if (commandType.equalsIgnoreCase("show")) {

      if (args.length < 2) {
        return printErrAndReturn(PortalShellConstants.InvalidCommandFormat(commandType))
      }

      showType = args(1);
      tViewName = args(2);
      var cmd = retrieveCommand(tViewName).asInstanceOf[CreateViewCommand];
      if (cmd == null) return false;

      if (showType.equalsIgnoreCase("plan")) {
        val plan = cmd.getPlanDescription();
        printInfoMessage(plan);

      } else if (showType.equalsIgnoreCase("view")) {
        val view = cmd.getPortalQuery();
        printInfoMessage(view);

      } else {
        return printErrAndReturn(PortalShellConstants.UnsupportedErrorText())
      }

    } else if (commandType.equalsIgnoreCase("help")) {
      var helpType: String = null;

      if (args.length > 1) {
        helpType = args(1);
      }

      command = new HelpCommand(portalContext, commandNum, helpType);
      command.execute();

    } else if (commandType.equalsIgnoreCase("select")) {
      //TODO: sql statement processing

    } else if (commandType.equalsIgnoreCase("info")) {
      if (args.length < 2 && !args(1).equalsIgnoreCase("portalShell")) {
        return printErrAndReturn(PortalShellConstants.InvalidCommandFormat(commandType))
      }

    } else {
      return printErrAndReturn(PortalShellConstants.UnsupportedErrorText())
    }

    return true;
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

  def retrieveCommand(tViewName: String): PortalCommand = {
    var command: PortalCommand = null;

    if (commandList.contains(tViewName)) {
      command = commandList.get(tViewName).getOrElse(null);
    } else {
      printErrMessage(PortalShellConstants.UnsupportedErrorText());
    }

    return command;
  }

  def retrieveQuery(command: String): String = {
    val queryRegex = "\\(.*\\)$"
    val pattern = new Regex(queryRegex);
    var res = (pattern findFirstIn command).getOrElse(null)

    if (res != null) {
      res = res.drop(1).dropRight(1); //replace beginning and ending "(" and ")"
    }
    return res
  }

  def printProgramStart() = {
    printf(String.format("\n%s\n\t\t\t%s\n\t\t%s\n%s\n",
      "===============================================================================",
      "Welcome to the Portal Shell!",
      "    cs.drexel.edu.dbgroup.temporalgraph",
      "==============================================================================="))
  }

  def printInfoMessage(message: String) = {
    //print info message
    println(PortalShellConstants.InfoText(message));
  }

  def printErrMessage(message: String) = {
    //print error message
    println(PortalShellConstants.ErrText(message));
  }

  def printErrAndReturn(errMessage: String): Boolean = {
    printErrMessage(errMessage);
    return false;
  }

}
