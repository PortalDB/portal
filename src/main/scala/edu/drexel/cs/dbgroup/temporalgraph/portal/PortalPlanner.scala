package edu.drexel.cs.dbgroup.temporalgraph.portal

import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph.plans.PortalPlan
import edu.drexel.cs.dbgroup.temporalgraph.plans._
import edu.drexel.cs.dbgroup.temporalgraph.expressions._

import org.apache.spark.sql.catalyst.planning.QueryPlanner
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.expressions.{Expression, Alias, NamedExpression}

class PortalPlanner extends QueryPlanner[PortalPlan] {
  self: PortalPlanner =>
  //this class pics the data structure(s) to use
  //as well as number of partitions, etc.

  def strategies: Seq[Strategy] = (
    PickPartitionStrategyAndStructure :: 
    BasicOperators ::
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
      case lg @ logical.LoadGraphWithSchema(spec, url, start, end) =>
        physical.PhysicalGraph(lg.output, url, start, end, defaultDS) :: Nil
      case l @ logical.LoadGraph(url, start, end) =>
        physical.PhysicalGraph(l.output, url, start, end, defaultDS) :: Nil
      case _ => Nil
    }
  }

  //this is a basic replacement of logical to physical
  object BasicOperators extends Strategy {
    def convertAggregateExpressions(seq: Seq[Expression]): Seq[NamedExpression] = {
      //during analysis these expressions are already replaced
      //by proper aggregate functions
      seq.map(x => x match {
        case sa: StructuralAggregate => Alias(sa, sa.toString)()
        case al @ Alias(sa: StructuralAggregate, i) => al
        case _ => throw new IllegalStateException(
          "all structural aggregation expressions should have been replaced by their appropriate functions by the optimizer")
      })
    }

    def apply(plan: LogicalPlan): Seq[PortalPlan] = plan match {
      case logical.TGroup(res, vsem, esem, vaggs, eaggs, child) =>
        physical.TGroup(res, vsem, esem, convertAggregateExpressions(vaggs), convertAggregateExpressions(eaggs), planLater(child)) :: Nil
      case _ => Nil
    }
  }

}
