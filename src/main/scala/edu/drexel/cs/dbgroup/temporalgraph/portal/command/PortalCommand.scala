package edu.drexel.cs.dbgroup.temporalgraph.portal.command;

abstract class PortalCommand(text: String, num : Int) {
  var timeExecuted: Long = System.currentTimeMillis();
  val commandText: String = text;
  val commandNum: Int = num; 

  def describeCommand();
  def executeCommand();
  def verifySyntax(command: String): Boolean;

  def getExecutionTime(): Long = {
    return timeExecuted;
  }

}
