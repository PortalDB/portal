package edu.drexel.cs.dbgroup.temporalgraph.portal.command;

import edu.drexel.cs.dbgroup.temporalgraph.PortalContext;
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalParser;
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalShellConstants;
import edu.drexel.cs.dbgroup.temporalgraph.TemporalGraphWithSchema;

class CreateViewCommand(portalContext: PortalContext, commandNum: Int, portalQuery: String, tViewName: String,
  isMaterialized: Boolean) extends PortalCommand(portalContext, commandNum) {

  //val schemaDescriptionFormat: String = "TView \'%s\' schema => %s";

  // begin primary constructor definition  
  if (portalQuery == null || portalQuery.isEmpty()) {
    throw new Exception(PortalShellConstants.InvalidQuerySyntax());
  };

  // begin method implementation  
  
  /*
   * TODO: what does this method return? TemporalGraph or PortalPlan?
   * or create the TemporalGraph in the PortalShell class?
   */
  override def execute() = {
    try {
      var pcontext = getPortalContext();
      queryExec = pcontext.executePortal(portalQuery);
      tempGraph = queryExec.toTGraph;

      //register a view with a name
      portalContext.catalog.registerTable(List(tViewName), queryExec.analyzed);

      if (isMaterialized) {
        tempGraph.materialize();
      }

      attributes += ("tViewName" -> tViewName)

    } catch {
      case ex: Exception => {
        //FIXME: handle exception correctly 
        throw new Exception(ex);
      };
    }
  };

  def describeSchema(): String = {
    var pcontext = getPortalContext();

    if (pcontext == null) {
      throw new Exception(PortalShellConstants.InvalidPortalContext());
    };

    //var description: String = String.format(schemaDescriptionFormat, tViewName, tempGraph.getSchema().toString());
    var description = tempGraph.getSchema().toString();
    return description;
  };

  def getPlanDescription(): String = {
    if (queryExec == null) {
      throw new Exception(PortalShellConstants.InvalidExecutionPlan());
    };

    return queryExec.optimizedPlan.toString();
  };

  def getPortalQuery(): String = {
    return portalQuery;
  };

  // end method implementation 
}