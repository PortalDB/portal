package edu.drexel.cs.dbgroup.temporalgraph.portal.command;

import edu.drexel.cs.dbgroup.temporalgraph.PortalContext;
import edu.drexel.cs.dbgroup.temporalgraph.plans.PortalPlan;
import edu.drexel.cs.dbgroup.temporalgraph.TemporalGraphWithSchema;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

abstract class PortalCommand(portalContext: PortalContext) {
  var timeExecuted: Long = System.currentTimeMillis();
  var tempGraph: TemporalGraphWithSchema = null;
  var queryExec: PortalContext#QueryExecution = null;
  var attributes: Map[String, String] = Map();
  
  //to be implemented by subclasses
  def execute();
  
  // begin method implementation
  def getExecutionTime(): Long = {
    return timeExecuted;
  }
  
  def getAttributes(): Map[String, String] = {
     return attributes;
  }
  
}
