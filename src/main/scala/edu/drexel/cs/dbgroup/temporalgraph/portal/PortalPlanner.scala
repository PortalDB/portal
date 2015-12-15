package edu.drexel.cs.dbgroup.temporalgraph.portal

import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph.plans.PortalPlan
import edu.drexel.cs.dbgroup.temporalgraph.plans._

import org.apache.spark.sql.catalyst.planning.QueryPlanner
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class PortalPlanner extends QueryPlanner[PortalPlan] {
  self: PortalPlanner =>
  //this class pics the data structure(s) to use
  //as well as number of partitions, etc.

  def strategies: Seq[Strategy] = (
    PickPartitionStrategyAndStructure :: 
    Nil)

  //TODO: replace strings for data structure types with
  //something better
  //TODO: more advanced rules for picking the data structure
  //TODO: pick partition strategy
  object PickPartitionStrategyAndStructure extends Strategy {
    val defaultDS = "SG"
    def apply(plan: LogicalPlan): Seq[PortalPlan] = plan match {
      case logical.LoadGraphFullInfo(spec, url, start, end, snapP, aggsP) =>
        if (snapP)
          physical.PhysicalGraph(spec, url, start, end, "SG") :: Nil
        else if (aggsP)
          physical.PhysicalGraph(spec, url, start, end, "OGC") :: Nil
        else
          physical.PhysicalGraph(spec, url, start, end, defaultDS) :: Nil
      case logical.LoadGraphWithDate(spec, url, start, end) =>
        physical.PhysicalGraph(spec, url, start, end, defaultDS) :: Nil
      case logical.LoadGraphWithSchema(spec, url) =>
        physical.PhysicalGraph(spec, url, LocalDate.MIN, LocalDate.MAX, defaultDS) :: Nil
      case l @ logical.LoadGraph(url) =>
        physical.PhysicalGraph(l.output, url, LocalDate.MIN, LocalDate.MAX, defaultDS) :: Nil
      case _ => Nil
    }
  }

}
