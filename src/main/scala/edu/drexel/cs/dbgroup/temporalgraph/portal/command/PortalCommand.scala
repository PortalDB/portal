package edu.drexel.cs.dbgroup.temporalgraph.portal.command;

import edu.drexel.cs.dbgroup.temporalgraph.PortalContext;
import edu.drexel.cs.dbgroup.temporalgraph.plans.PortalPlan;
import edu.drexel.cs.dbgroup.temporalgraph.TemporalGraphWithSchema;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

abstract class PortalCommand(portalContext: PortalContext, commandNum : Int) {
  var timeExecuted: Long = System.currentTimeMillis();
  var verifierRes: Tuple2[Boolean, String] = null;
  var logicalPlan: LogicalPlan = null;
  var portalPlan: PortalPlan = null;
  var tempGraph: TemporalGraphWithSchema = null;
  
  //to be implemented by subclasses
  def describe(): String;
  def execute(): TemporalGraphWithSchema;
  def verifySyntax(): Boolean;
  
  // begin method implementation
  def getExecutionTime(): Long = {
    return timeExecuted;
  }

}
