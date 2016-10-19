package edu.drexel.cs.dbgroup.temporalgraph.portal

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.control._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.portal.command._
import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Stack
import scala.tools.jline.console.ConsoleReader
import scala.util.matching.Regex
import scala.util.control._
import scala.util.Properties

object PortalShell {
  var numGenericTViews: Integer = 0;
  var commandList: Map[String, PortalCommand] = Map(); //tViewName -> PortalCommand
  var portalContext: PortalContext = null;
  var hideCharactersInTerminal = false;

  def main(args: Array[String]) = {

    //note: this does not remove ALL logging  
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    var graphType: String = "SG";
    var data = "";
    var partitionType: PartitionStrategyType.Value = PartitionStrategyType.None;
    var runWidth: Int = 2;
    var query: Array[String] = Array.empty;

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
	case "--hideCharactersInTerminal" =>
	  hideCharactersInTerminal = true
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

    val sqlContext = ProgramContext.getSession
    //TODO: remove hard-coding of this parameter. currently it is 1024x1024x16, i.e. 16mb
    sqlContext.conf.set("spark.sql.files.maxPartitionBytes", "16777216")

    GraphLoader.setGraphType(graphType)
    GraphLoader.setStrategy(partitionType)
    GraphLoader.setRunWidth(runWidth)

    val startAsMili = System.currentTimeMillis()
    startConsole()
    val stopAsMili = System.currentTimeMillis()
    val runTime = stopAsMili - startAsMili

    println(f"Final Runtime: $runTime%dms")
    sc.stop
  }

  def startConsole() = {
    val consoleReader = new ConsoleReader();
    val loop = new Breaks;
    printProgramStart();

    loop.breakable {
      while (true) {
	var line = "";
	if(!hideCharactersInTerminal){
	  line = consoleReader.readLine("portal> ");
	}
	else{
	  line = consoleReader.readLine("", new Character(0));
	}
        if (checkQuit(line)) {
          loop.break;
        }

        while (!isLineEnd(line)) {
	  if(!hideCharactersInTerminal){
            line += consoleReader.readLine(">");
          }
	  else{
            line += consoleReader.readLine(">", new Character(0));
	  }
        }

        line = line.dropRight(1).trim(); //remove terminating "\" and whitespace

        if (parseCommand(line) == false) {
          printErr(PortalShellConstants.CommandUnsuccesful());
        }
      }
    }

    println("Exiting Portal Shell...\n")
  }

  def parseCommand(line: String): Boolean = {
    var command: PortalCommand = null;
    var args = line.split(" ");
    var commandType: String = args(0);
    var tViewName: String = null;
    var portalQuery: String = null;
    //var sqlInterface: SQLInterface = null;
    var isMaterialized: Boolean = false;
    var showType: String = null;
    var isNoop: Boolean = false;

    if (commandType.equalsIgnoreCase("create")) {
      tViewName = args(2);
      portalQuery = retrieveQuery(line);
      println ("Portal Query Extracted -->" + portalQuery);

      //TODO: confirm that second arg is "view"

      if (args(1).equalsIgnoreCase("materialized")) {
        isMaterialized = true;
        tViewName = args(3);
      }

      return createAndSaveView(portalContext, portalQuery, tViewName, isMaterialized);

    } else if (commandType.equalsIgnoreCase("show")) {

      if (args.length < 3) {
        return printErrAndReturnFalse(PortalShellConstants.InvalidCommandFormat(commandType))
      }

      showType = args(1);
      tViewName = args(2);

      if (!tViewExists(tViewName)) {
        return false
      }

      var cmd = retrieveCommand(tViewName).asInstanceOf[CreateViewCommand];

      if (showType.equalsIgnoreCase("plan")) {
        val plan = cmd.getPlanDescription();
        printInfo(plan);

      } else if (showType.equalsIgnoreCase("view")) {
        val view = cmd.getPortalQuery();
        printInfo(view);

      } else {
        return printErrAndReturnFalse(PortalShellConstants.UnsupportedErrorText())
      }

    } else if (commandType.equalsIgnoreCase("help")) {
      var helpType: String = null;

      if (args.length > 1) {
        helpType = args(1);
      }

      command = new HelpCommand(portalContext, helpType);
      command.execute();
/* until we figure the no schema to schema translation issue, no conversion to SQL possible
    } else if (commandType.equalsIgnoreCase("select")) {      
      try {
        sqlInterface = new SQLInterface();
        var tViewName: String = null;
        var queryRewrite: String = line;  
        
        if(containsPortalQuery(line)){
          queryRewrite = parseQueryAndRewriteLine(line);
        }
        
        tViewName = extractSimpleTViewName(queryRewrite);
        
        if (tViewName == null || tViewName.isEmpty() || !tViewExists(tViewName)) {
          return printErrAndReturnFalse(PortalShellConstants.TViewDoesNotExist(tViewName));
        }

        //println ("queryRewrite: " + queryRewrite + " ; tViewName: " + tViewName)
        printInfo(PortalShellConstants.StatusExecutingSQL(tViewName));
        
        var cmd = retrieveCommand(tViewName).asInstanceOf[CreateViewCommand];
        var res = sqlInterface.runSQLQuery(queryRewrite, cmd.tempGraph);
        printInfo("Res from SQL:" + res.toString());

      } catch {
        case ex: Exception => {
          return printErrAndReturnFalse(PortalShellConstants.StatusSQLExecutionFailed(ex.getMessage()));
        }
      }
 */
    } else if (commandType.equalsIgnoreCase("info")) {
      if (args.length < 2 && !args(1).equalsIgnoreCase("portalShell")) {
        return printErrAndReturnFalse(PortalShellConstants.InvalidCommandFormat(commandType))
      }

    } else {
      return printErrAndReturnFalse(PortalShellConstants.UnsupportedErrorText())
    }

    return true;
  }

  def extractSimpleTViewName(line: String): String = {
    var lineArgs = line.split(" ");
    var tViewIndex: Integer = -1;

    for (i <- 0 until lineArgs.length) {
      if (lineArgs(i).equalsIgnoreCase("from")) {
        //TODO: more vigorous check e.g parentheses in tViewName
        tViewIndex = i + 1;
      }
    }

    if (tViewIndex < 0 || tViewIndex >= lineArgs.length) {
      //TODO: error checking
      return null
    }

    var tViewName = lineArgs(tViewIndex);
    return tViewName.split('.')(0);
  }

  def parseQueryAndRewriteLine(line: String): String = {
    var tViewStack = new Stack[String]();  
    var portalQuery: String = line;
    var lineRewrite: String = null;

    while(portalQuery != null){
      tViewStack.push(portalQuery);
      portalQuery = retrieveQuery(portalQuery);      
    }
        
    while(tViewStack.size > 1){
      numGenericTViews += 1;
      var currentView = tViewStack.top;
      var tViewName = PortalShellConstants.GenericTViewName(numGenericTViews);
      var isCreated = createAndSaveView(portalContext, currentView, tViewName, false); //create unmaterialized tview
      
      if (!isCreated) {
        numGenericTViews -= 1;
        throw new Exception;
      }
      
      tViewStack.pop;
      lineRewrite = tViewStack.pop;

      lineRewrite = replaceQueryWithTView(lineRewrite, tViewName);
      tViewStack.push(lineRewrite);
      portalQuery = retrieveQuery(lineRewrite);
    }
    
    return tViewStack.top;
  }

  def createAndSaveView(portalContext: PortalContext, portalQuery: String,
    tViewName: String, isMaterialized: Boolean): Boolean = {

    try {
      printInfo(PortalShellConstants.StatusCreatingTView(tViewName));

      if (commandList.contains(tViewName)) {
        return printErrAndReturnFalse(PortalShellConstants.TViewExistsMessage(tViewName))
      }

      var command = new CreateViewCommand(portalContext, portalQuery, tViewName, isMaterialized);
      command.execute();

      //add new command to list of commands
      commandList += (tViewName -> command)
      printInfoAndReturnTrue(PortalShellConstants.TViewCreationSuccess(tViewName))

    } catch {
      case ex: Exception => {
        printErrAndReturnFalse(PortalShellConstants.TViewCreationFailed(tViewName, ex.getMessage()))
        
      }
    }
  }
  
  def containsPortalQuery(line: String): Boolean = {
    return (retrieveQuery(line) != null);
  }

  def isLineEnd(line: String): Boolean = {
    if (line.takeRight(1) == "\\") {
      return true;
    }
    return false;
  }

  def checkQuit(line: String): Boolean = {
    if (line == "q\\" | line == Properties.lineSeparator) {
      return true;
    }
    return false;
  }

  def tViewExists(tViewName: String): Boolean = {
    return commandList.contains(tViewName);
  }

  def retrieveCommand(tViewName: String): PortalCommand = {
    return commandList.get(tViewName).getOrElse(null);
  }

  def retrieveQuery(command: String): String = {
    var res = (PortalShellConstants.QueryRegex findFirstIn command).getOrElse(null);

    if (res != null) {
      res = res.drop(1).dropRight(1); //replace beginning and ending "{" and "}"
    }
    return res
  }

  def replaceQueryWithTView(line: String, tViewName: String): String = {
    var res = PortalShellConstants.QueryRegex.replaceFirstIn(line, tViewName);
    return res
  }

  def printProgramStart() = {
    printf(String.format("\n%s\n\t\t\t%s\n\t\t%s\n%s\n",
      "===============================================================================",
      "Welcome to the Portal Shell!",
      "    cs.drexel.edu.dbgroup.temporalgraph",
      "==============================================================================="))
  }

  def printErr(message: String) = {
    println(PortalShellConstants.ErrText(message));
  }

  def printInfo(message: String) = {
    println(PortalShellConstants.InfoText(message));
  }

  def printErrAndReturnFalse(message: String): Boolean = {
    println(PortalShellConstants.ErrText(message));
    return false;
  }

  def printInfoAndReturnTrue(message: String): Boolean = {
    println(PortalShellConstants.InfoText(message));
    return true;
  }

}
