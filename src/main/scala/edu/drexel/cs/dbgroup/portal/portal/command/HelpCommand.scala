package edu.drexel.cs.dbgroup.portal.portal.command;

import edu.drexel.cs.dbgroup.portal.PortalContext;
import edu.drexel.cs.dbgroup.portal.portal.PortalParser;
import edu.drexel.cs.dbgroup.portal.portal.PortalShellConstants;
import edu.drexel.cs.dbgroup.portal.portal.command.PortalCommandType._;

class HelpCommand(portalContext: PortalContext, commandName: String)
  extends PortalCommand(portalContext) {
  
  override def execute() = {
    if (commandName == null){
      println(PortalCommandType.describeAll());
    } else {
      println("Help text for " + commandName)
      //println(PortalCommandType.describeCommand(commandName));
    }
  }
}
