package edu.drexel.cs.dbgroup.temporalgraph.portal.command;

import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalParser;
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalShellConstants;

class CreateCommand(commandNum: Int, portalQuery: String, tViewName: String, isMaterialized: Boolean) extends PortalCommand(commandNum) {

  if (portalQuery == null || portalQuery.isEmpty()) {
    throw new Exception(PortalShellConstants.InvalidSyntaxText());
  };

  verifySyntax();

  override def describe() = {
    println("This is a Portal \"Create\" command:");
  };

  override def execute(): Boolean = {
    println("Executing \'Create\' command:");

    try {
      logicalPlan = PortalParser.parse(portalQuery);

    } catch {
        case ex: Exception => {
          println(PortalShellConstants.ErrText(ex.getMessage()));
        };
        
        return false;
      }

    return true;
  };

  override def verifySyntax(): Boolean = {
    println("[info] Checking \'Create\' command syntax of: " + portalQuery)

    return true;
  };

}