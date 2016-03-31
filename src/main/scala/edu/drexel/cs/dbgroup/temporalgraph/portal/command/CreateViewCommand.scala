package edu.drexel.cs.dbgroup.temporalgraph.portal.command;

import org.apache.spark.sql.catalyst.TableIdentifier
import edu.drexel.cs.dbgroup.temporalgraph.PortalContext;
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalParser;
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalShellConstants;
import edu.drexel.cs.dbgroup.temporalgraph.TemporalGraphWithSchema;

import org.apache.spark.sql.ModifierWorkaround

class CreateViewCommand(portalContext: PortalContext, portalQuery: String, tViewName: String,
  isMaterialized: Boolean) extends PortalCommand(portalContext) {

  //val schemaDescriptionFormat: String = "TView \'%s\' schema => %s";

  // begin primary constructor definition  
  if (portalQuery == null || portalQuery.isEmpty()) {
    throw new Exception(PortalShellConstants.InvalidQuerySyntax());
  };

  // begin method implementation  
  override def execute() = {
    try {
      queryExec = portalContext.executePortal(portalQuery);
      tempGraph = queryExec.toTGraph;

      //register a view with a name
      portalContext.catalog.registerTable(ModifierWorkaround.makeTableIdentifier(tViewName), queryExec.analyzed);

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
