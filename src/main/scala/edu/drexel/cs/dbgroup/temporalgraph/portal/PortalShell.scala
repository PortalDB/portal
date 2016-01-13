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

//    printf("#%d --> %s\n", commandNum, line);

    if (commandType.equalsIgnoreCase("create")) {
      tViewName = args(2);
      portalQuery = retrieveQuery(line);
      printf("portalQuery retrieved from command: %s\n", portalQuery);

      if (args(1).equalsIgnoreCase("materialized")) {
        isMaterialized = true;
        tViewName = args(4);
      }

      try {
        command = new CreateViewCommand(portalContext, commandNum, portalQuery, tViewName, isMaterialized);
        val result = command.execute();
        
        if (commandList.contains(tViewName)){
          //FIXME: what is the correct action if tViewNmae already exists
          printf("TView\'%s\' already exists, replacing it.\n", tViewName)
        }
        
        //add new command to list of commands
        commandList += (tViewName -> command)
        
      } catch {
        case ex: Exception => {
          printErrMessage(ex.getMessage());
          command = null
        }
      }
      //println("\ntViewName --> " + tViewName);

    } else if (commandType.equalsIgnoreCase("describe")) {
      
      if(args.length < 2){
        printErrMessage(PortalShellConstants.InvalidCommandFormat(commandType));
        return false
      }
      
      tViewName = args(1);
      
      if (commandList.contains(tViewName)){
          command = commandList.get(tViewName).getOrElse(null);
      } else {
        printErrMessage(PortalShellConstants.UnsupportedErrorText());
      }
      
      val description = command.describe();

    } else if (commandType.equalsIgnoreCase("show")) {

      if(args.length < 2){
        printErrMessage(PortalShellConstants.InvalidCommandFormat(commandType));
        return false
      }
      
      showType = args(1);
      
      if(showType.equalsIgnoreCase("plan")){
        //show plan
        
      }else if(showType.equalsIgnoreCase("view")){
        //show view
        
      } else {
        printErrMessage(PortalShellConstants.UnsupportedErrorText());
        return false
      }
      
    } else if (commandType.equalsIgnoreCase("help")){
      var helpType: String = null;
      
      if(args.length > 1){
        helpType = args(1);
      }
      
      command = new HelpCommand(portalContext, commandNum, helpType);
      var result = command.describe();
      println(result);
      
    } else if (commandType.equalsIgnoreCase("select")){
      //sql statement
      
    } else if (commandType.equalsIgnoreCase("info")){
      if(args.length < 2 && !args(1).equalsIgnoreCase("portalShell")){
        printErrMessage(PortalShellConstants.InvalidCommandFormat(commandType));
        return false
      }
      
      
    } else {
      printErrMessage(PortalShellConstants.UnsupportedErrorText());
      return false
    }

    if (command == null) {
      return false;
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
  
  def printErrMessage(message: String) = {
    //print error message
    println(PortalShellConstants.ErrText(message));
  }
}
