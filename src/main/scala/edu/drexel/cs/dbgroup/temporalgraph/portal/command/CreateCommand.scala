package edu.drexel.cs.dbgroup.temporalgraph.portal.command;

class CreateCommand(text: String, commandNum: Int, viewName: String, materialize: Boolean) extends PortalCommand(text, commandNum) {
  val tViewName: String = viewName;
  var isMaterialized: Boolean = materialize;
  
  override def describeCommand() = {
    println("This is a Portal \"Create\" command");
  }

  override def executeCommand() = {

  }

  override def verifySyntax(command: String): Boolean = {
    println("[info] Checking \'Create\' command syntax: ", command)

    return true;
  }

}