package edu.drexel.cs.dbgroup.temporalgraph.portal;

object PortalShellConstants {
  def InfoText(message: String): String = { String.format("[info] %s", message) };
  def ErrText(message: String): String = { String.format("[error] %s", message) };
  def WarnText(message: String): String = { String.format("[warn] %s", message) };
  
  def UnsupportedErrorText(): String = { "Unsupported Command" };
  def UnsupportedErrorText(message: String): String = { String.format("Unsupported Command: %s", message) };

  def InvalidQuerySyntax(): String = { "Invalid Command Syntax" };
  def InvalidQuerySyntax(message: String): String = { String.format("Invalid Command Syntax: %s", message) };
  
  def InvalidPortalContext(): String = {"Invalid Portal Context"};
  def InvalidExecutionPlan(): String = {"Invalid Execution Plan"};
  def InvalidCommandFormat(): String = {"Invalid Command Format"};
  def InvalidCommandFormat(message: String): String = {String.format("Invalid Command Format: use \'help %s\' command description", message) };
  
  def TViewDoesNotExist(tViewName: String): String = { String.format("TView \'%s\' Does Not Exist", tViewName)};

}