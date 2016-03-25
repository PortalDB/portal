package edu.drexel.cs.dbgroup.temporalgraph.portal;

import scala.util.matching.Regex

object PortalShellConstants {
  def GenericTViewName(viewNumber: Integer): String = { String.format("portalTView#%d", viewNumber) };
  def QueryRegex(): Regex = new Regex("\\{.*\\}");
  
  //tView creation
  def TViewCreationSuccess(tViewName: String): String = { String.format("TView \'%s\' created.", tViewName) };
  def TViewCreationFailed(tViewName: String, message: String): String = { String.format("Unable to create TView \'%s\'.%s", tViewName, message) };
  def TViewExistsMessage(tViewName: String): String = { String.format("TView \'%s\' already exists, replacing it.", tViewName) };
  
  //system message types
  def InfoText(message: String): String = { String.format("[info] %s", message) };
  def ErrText(message: String): String = { String.format("[error] %s", message) };
  def WarnText(message: String): String = { String.format("[warn] %s", message) };

  def UnsupportedErrorText(): String = { "Unsupported Command" };
  def UnsupportedErrorText(message: String): String = { String.format("Unsupported Command: %s", message) };

  def InvalidQuerySyntax(): String = { "Invalid Command Syntax" };
  def InvalidQuerySyntax(message: String): String = { String.format("Invalid Command Syntax: %s", message) };

  def InvalidPortalContext(): String = { "Invalid Portal Context" };
  def InvalidExecutionPlan(): String = { "Invalid Execution Plan" };
  def InvalidCommandFormat(): String = { "Invalid Command Format" };
  def InvalidCommandFormat(message: String): String = { String.format("Invalid Command Format: use \'help %s\' command description", message) };

  def TViewDoesNotExist(tViewName: String): String = { String.format("TView \'%s\' Does Not Exist", tViewName) };
  def InternalError(message: String): String = { String.format("Internal Error: %s", message) };
  def TViewExtractionFailed(message: String): String = { String.format("Unable to extract TViews from the query: %s", message) };
  def CommandUnsuccesful(): String = { "Command execution was unsuccessful" };

  //status messages
  def StatusCreatingTView(tViewName: String): String = { String.format("Creating TView \'%s\'...", tViewName) };
  def StatusExecutingSQL(tViewName: String): String = { String.format("Executing SQL on TView \'%s\'...", tViewName) };
  def StatusSQLExecutionFailed(): String = { String.format("Executing SQL command failed") };
  def StatusSQLExecutionFailed(message: String): String = { String.format("Executing SQL command failed with error: %s", message) };

}