package edu.drexel.cs.dbgroup.temporalgraph.portal

import edu.drexel.cs.dbgroup.temporalgraph.plan.PortalPlan

import org.apache.spark.sql.catalyst.planning.QueryPlanner
import org.apache.spark.sql.Strategy

class PortalPlanner extends QueryPlanner[PortalPlan] {
  //this class pics the data structure(s) to use
  //as well as number of partitions, etc.

  //TODO!
  def strategies = Seq()
}
