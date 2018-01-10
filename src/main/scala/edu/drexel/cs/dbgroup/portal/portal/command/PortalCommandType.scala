package edu.drexel.cs.dbgroup.portal.portal.command;

import scala.collection.SortedSet;
import scala.collection.immutable.TreeSet;
import scala.collection.immutable.ListSet;

object PortalCommandType {
  val commandPrintfFormat: String = "%-60s%s\n";
  val shellHelpFormat: String = "%s\n\n%s\n";

  sealed abstract class CommandType(
    val name: String);

  case object Create extends CommandType("create");
  case object Describe extends CommandType("describe");
  case object Show extends CommandType("show");
  case object Sql extends CommandType("sql");
  case object Help extends CommandType("help");

  val commandTypes: Set[CommandType] = ListSet(Create, Describe, Show, Sql, Help);

  sealed abstract class Command(
    val name: String,
    val usage: String,
    val cType: CommandType,
    val description: String) extends Ordered[Command] {

    def getDescription(): String = {
      var quotedStr = String.format("\'%s\'", this.usage)
      return String.format(commandPrintfFormat, quotedStr, this.description);
    }

    // @override: return 0 if the same, negative if this < that, positive if this > that
    def compare(that: Command) = {
      if (this.name == that.name) { 0 }
      else if (this.name > that.name) { 1 }
      else { -1 }
    }
  }

  case object CreateView extends Command(
    "Create View",
    "create tview <name> as ( portal query )",
    Create,
    "Creates a new tView");

  case object CreateMatView extends Command(
    "Create aterialized View",
    "create materialized tview <name> as ( portal query )",
    Create,
    "Creates a new materialized tView");

  case object DescribeView extends Command(
    "Describe View",
    "describe <tViewName>",
    Describe,
    "Describes [shows schema] a previously created tView");

  case object ShowView extends Command(
    "Show View",
    "show view <tViewName>",
    Show,
    "Shows a tViewName");

  case object ShowPlan extends Command(
    "Show Plan",
    "show plan <tViewName>",
    Show,
    "Shows the query execution plan");

  case object HelpAll extends Command(
    "Help",
    "help",
    Help,
    "Shows all commands that are available");

  case object HelpWithCommand extends Command(
    "Help With Command",
    "help <commandName>",
    Help,
    "Describes the function of the given command");

  val commands: SortedSet[Command] = TreeSet(CreateView, CreateMatView, DescribeView, ShowView,
    ShowPlan, HelpAll, HelpWithCommand);

  
  //method implementations
  def describeCommand(commandName: String) = {
    //TODO: to implement
  };
  
  def describeAll(): String = {
    var res: String = "";
    commands.foreach(res += _.getDescription)

    return String.format(shellHelpFormat, shellHeader(), res);
  };
  
  def verifyCommand(commandName: String) = {
    //TODO: to implement
  };
  
  def shellHeader(): String = {
    return String.format("%s\n%s\n%s\n%s",
      "PortalShell, version 1.0.",
      "These shell commands are defined internally. Type 'help' to see this list.",
      "Type 'help name' to find out more about the function 'name'.",
      "Use 'info portalshell' to find out more about the shell in general.");
  };

}