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
  val GenericTViewName: String = "TView%d";
  val queryRegex = new Regex("\\{.*\\}");

  var numGenericViews: Integer = 0;
  var commandList: Map[String, PortalCommand] = Map(); //tViewName -> PortalCommand
  var portalContext: PortalContext = null;

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
        var line = consoleReader.readLine("portal> ");

        if (checkQuit(line) == true) {
          loop.break;
        }

        while (!isLineEnd(line)) {
          line += consoleReader.readLine(">");
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
    var sqlInterface: SQLInterface = null;
    var isMaterialized: Boolean = false;
    var showType: String = null;
    var isNoop: Boolean = false;

    if (commandType.equalsIgnoreCase("create")) {
      tViewName = args(2);
      portalQuery = retrieveQuery(line);

      //TODO: confirm that second arg is "view"

      if (args(1).equalsIgnoreCase("materialized")) {
        isMaterialized = true;
        tViewName = args(3);
      }

      return createAndSaveView(portalContext, portalQuery, tViewName, isMaterialized);

    } else if (commandType.equalsIgnoreCase("describe")) {

      if (args.length < 2) {
        return printErrAndReturnFalse(PortalShellConstants.InvalidCommandFormat(commandType))
      }

      tViewName = args(1);

      if (!tViewExists(tViewName)) {
        return false
      }

      var cmd = retrieveCommand(tViewName).asInstanceOf[CreateViewCommand];

      val description = cmd.describeSchema();
      printInfo(description);

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

    } else if (commandType.equalsIgnoreCase("select")) {
      var queryRewrite: String = null;
      
      try {

        sqlInterface = new SQLInterface();
        portalQuery = retrieveQuery(line);

        if (portalQuery == null) {
          tViewName = extractTView(line, false)(0);
          queryRewrite = line;
          
        } else {
          var tViews = extractTView(portalQuery, true);

          if (tViews.isEmpty) {
            return printErrAndReturnFalse(PortalShellConstants.TViewExtractionFailed(portalQuery))
          }

          tViewName = tViews(0);
          queryRewrite = rewriteQueryWithTView(line, tViewName);
          //printInfo("revisedQuery --> " + queryRewrite);
        }

        if (tViewName == null || tViewName.isEmpty()) {
          return printErrAndReturnFalse(PortalShellConstants.TViewDoesNotExist(tViewName))
        }

        if (!tViewExists(tViewName)) {
          return false
        }

        
        var cmd = retrieveCommand(tViewName).asInstanceOf[CreateViewCommand];
        
        printInfo(PortalShellConstants.StatusExecutingSQL(tViewName));
        var res = sqlInterface.runSQLQuery(queryRewrite, cmd.tempGraph)

        printInfo("Res from SQL:" + res.toString());

      } catch {
        case ex: Exception => {
          return printErrAndReturnFalse(PortalShellConstants.StatusSQLExecutionFailed(ex.getMessage()));
        }
      }

    } else if (commandType.equalsIgnoreCase("info")) {
      if (args.length < 2 && !args(1).equalsIgnoreCase("portalShell")) {
        return printErrAndReturnFalse(PortalShellConstants.InvalidCommandFormat(commandType))
      }

    } else {
      return printErrAndReturnFalse(PortalShellConstants.UnsupportedErrorText())
    }

    return true;
  }

  def extractSimpleTViewName(lineArgs: Array[String]): String = {
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

    return lineArgs(tViewIndex);
  }

  def extractTView(line: String, hasPortal: Boolean): List[String] = {
    //println("Extracting from line --> " + line)
    //println("hasPortal --> " + hasPortal)

    var args = line.split(" ");
    var tViews = new ListBuffer[String]();
    var fromIndices = new ListBuffer[Integer]();

    if (!hasPortal) { //simple query on a preview
      tViews += extractSimpleTViewName(args);
      return tViews.toList;
    }

    numGenericViews += 1;
    var tViewName = String.format(GenericTViewName, numGenericViews);

    //create unmaterialized tView
    var isCreated = createAndSaveView(portalContext, line, tViewName, false);
    //var isCreated: Boolean = true;

    if (isCreated) {
      tViews += tViewName;
    } else {
      numGenericViews -= 1;
    }

    return tViews.toList;
  }

  def createAndSaveView(portalContext: PortalContext, portalQuery: String,
    tViewName: String, isMaterialized: Boolean): Boolean = {

    try {
      printInfo(PortalShellConstants.StatusCreatingTView(tViewName));

      var command = new CreateViewCommand(portalContext, portalQuery, tViewName, isMaterialized);
      command.execute();

      if (commandList.contains(tViewName)) {
        //FIXME: what is the correct action if tViewName already exists
        return printErrAndReturnFalse(String.format(TViewExists, tViewName))
      }

      //add new command to list of commands
      commandList += (tViewName -> command)
      printInfoAndReturnTrue(String.format(TViewCreationSuccess, tViewName))

    } catch {
      case ex: Exception => {
        return printErrAndReturnFalse(String.format(TViewCreationFailed, tViewName, ex.getMessage()))
      }
    }
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

  def tViewExists(tViewName: String): Boolean = {
    return commandList.contains(tViewName);
  }

  def retrieveCommand(tViewName: String): PortalCommand = {
    return commandList.get(tViewName).getOrElse(null);
  }

  def retrieveQuery(command: String): String = {
    var res = (queryRegex findFirstIn command).getOrElse(null)

    if (res != null) {
      res = res.drop(1).dropRight(1); //replace beginning and ending "{" and "}"
    }
    return res
  }

  def rewriteQueryWithTView(line: String, tViewName: String): String = {
    var res = queryRegex.replaceFirstIn(line, tViewName);
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
