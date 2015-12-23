package edu.drexel.cs.dbgroup.temporalgraph.portal.command;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

abstract class PortalCommand(commandNum : Int) {
  var timeExecuted: Long = System.currentTimeMillis();
  var logicalPlan: LogicalPlan = null;
  
  def describe();
  def execute(): Boolean;
  def verifySyntax(): Boolean;

  def getExecutionTime(): Long = {
    return timeExecuted;
  }

}
