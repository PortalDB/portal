package edu.drexel.cs.dbgroup.temporalgraph.portal

import scala.collection.immutable

import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.analysis.{Analyzer,Catalog,FunctionRegistry}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class PortalAnalyzer(catalog: Catalog, registry: FunctionRegistry) extends Analyzer(catalog, registry, new SimpleCatalystConf(false)) {

  override lazy val batches: Seq[Batch] = Seq(
    Batch("Resolution", fixedPoint,
      ResolveRelations ::
      ResolveReferences ::
      ResolveFunctions ::
      Nil : _*)
  )

  override val extendedCheckRules: Seq[LogicalPlan => Unit] = Seq(
    CheckUnionCompatability(),
    CheckResolution()
  )

  case class CheckUnionCompatability() extends (LogicalPlan => Unit) {
    def apply(plan: LogicalPlan): Unit = {
      //TODO: verify that all unions/intersections are union compatible
    }
  }

  case class CheckResolution() extends (LogicalPlan => Unit) {
    def apply(plan: LogicalPlan): Unit = {
      //TODO: verify that aggregations go from smaller to larger valid resolutions
      //in tgroup ops
    }
  }

}
