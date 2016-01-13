package edu.drexel.cs.dbgroup.temporalgraph.portal.command;

import edu.drexel.cs.dbgroup.temporalgraph.PortalContext;
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalParser;
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalShellConstants;
import edu.drexel.cs.dbgroup.temporalgraph.portal.command.PortalCommandType._;

import edu.drexel.cs.dbgroup.temporalgraph.TemporalGraphWithSchema;


class HelpCommand(portalContext: PortalContext, commandNum: Int, commandName: String)
  extends PortalCommand(portalContext, commandNum) {
  
  def execute(): TemporalGraphWithSchema = {
    if (commandName != null){
      
    }
    
    return null;
  }
  
  def verifySyntax(): Boolean = {
    return true;
  }
  
  def describe(): String = {
    var result = PortalCommandType.describeAll();
    return result;
  }
}
