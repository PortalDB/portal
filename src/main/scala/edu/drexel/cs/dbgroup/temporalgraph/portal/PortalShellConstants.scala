package edu.drexel.cs.dbgroup.temporalgraph.portal;

object PortalShellConstants {
  def InfoText(message: String): String = { String.format("[info] %s", message) };
  def ErrText(message: String): String = { String.format("[error] %s", message) };
  def WarnText(message: String): String = { String.format("[warn] %s", message) };
  
  def UnsupportedErrorText(): String = { "Unsupported Command" };
  def UnsupportedErrorText(message: String): String = { String.format("Unsupported Command: %s", message) };

  def InvalidSyntaxText(): String = { "Invalid Command Syntax" };
  def InvalidSyntaxText(message: String): String = { String.format("Invalid Command Syntax: %s", message) };
}