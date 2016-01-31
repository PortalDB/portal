package edu.drexel.cs.dbgroup.temporalgraph.portal.command;

import edu.drexel.cs.dbgroup.temporalgraph.PortalContext;
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalParser;
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalShellConstants;
import edu.drexel.cs.dbgroup.temporalgraph.portal.command.PortalCommandType._;

import edu.drexel.cs.dbgroup.temporalgraph.TemporalGraphWithSchema;


class HelpCommand(portalContext: PortalContext, commandNum: Int, commandName: String)
  extends PortalCommand(portalContext, commandNum) {
  
  override def execute() = {
    if (commandName == null){
      println(PortalCommandType.describeAll());
    } else {
      println("Help text for " + commandName)
      //println(PortalCommandType.describeCommand(commandName));
    }
  }
}
