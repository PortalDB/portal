package edu.drexel.cs.dbgroup.temporalgraph.portal

import scala.collection.immutable

import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.{CatalystConf,TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{StructType,StructField}
import org.apache.spark.sql.catalyst.expressions.{UnaryExpression,NamedExpression,Alias,Expression}

import org.apache.spark.sql.ModifierWorkaround

import edu.drexel.cs.dbgroup.temporalgraph.plans._
//import edu.drexel.cs.dbgroup.temporalgraph.expressions._

class PortalAnalyzer(catalog: SessionCatalog, registry: FunctionRegistry, conf: CatalystConf) extends Analyzer(catalog, conf) {

  override lazy val batches: Seq[Batch] = Seq(
    Batch("Resolution", fixedPoint,
      ResolveGraphs ::
      ResolvePortalReferences ::
//      ResolvePortalFunctions ::
//      ResolvePortalAliases ::
//      Analytics ::
//      Aggregates ::
      Nil : _*)
  )

  override val extendedCheckRules: Seq[LogicalPlan => Unit] = Seq(
  )

  object ResolveGraphs extends Rule[LogicalPlan] {
    def getGraph(u: logical.UnresolvedGraph): LogicalPlan = {
      try {
        catalog.lookupRelation(ModifierWorkaround.makeTableIdentifier(u.graphIdentifier))
      } catch {
        case _: NoSuchTableException =>
          failAnalysis("no such view ${u.graphIdentifier}")
      }
    }
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case u: logical.UnresolvedGraph => getGraph(u)
    }
  }

  object ResolvePortalReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p: LogicalPlan if !p.childrenResolved => p

      case q: LogicalPlan =>
        q transformExpressionsUp  {
          case u @ UnresolvedAttribute(nameParts) =>
            // Leave unchanged if resolution fails.  Hopefully will be resolved next round.
            withPosition(u) { q.resolveChildren(nameParts, resolver).getOrElse(u) }
        }
    }
  }

  /**
   * Replaces [[UnresolvedFunction]]s with concrete [[Expression]]s.
   */
/*
  object ResolvePortalFunctions extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case q: LogicalPlan =>
        q transformExpressions {
          case u if !u.childrenResolved => 
            u // Skip until children are resolved.
          case u @ UnresolvedFunction(name, children, isDistinct) =>
            withPosition(u) {
              registry.lookupFunction(name, children) match {
                case other => 
                  other
              }
            }
        }
    }
  }
 */

  /**
    * Replaces [[UnresolvedAlias]]s with concrete aliases.
    * We do not allow anonymous fields.
  */
/*
  object ResolvePortalAliases extends Rule[LogicalPlan] {
    private def assignAliases(exprs: Seq[NamedExpression]) = {
      // The `UnresolvedAlias`s will appear only at root of a expression tree, we don't need
      // to traverse the whole tree.
      exprs.zipWithIndex.map {
        case (u @ UnresolvedAlias(child), i) =>
          child match {
            case _: UnresolvedAttribute => u
            case ne: NamedExpression => ne
            case sa: StructuralAggregate => 
              sa.child match {
                case na: NamedExpression => Alias(sa, na.name)()
                  //FIXME: we cannot allow anonymous fields like this
                case o => Alias(sa, s"_c$i")()
              }
            case e if !e.resolved => u
            case other => Alias(other, s"_c$i")()
          }
        case (other, _) => other
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case logical.TGroup(res, vsem, esem, vaggs, eaggs, child)
          if child.resolved && (vaggs.exists(_.isInstanceOf[UnresolvedAlias]) || eaggs.exists(_.isInstanceOf[UnresolvedAlias])) =>
        logical.TGroup(res, vsem, esem, assignAliases(vaggs), assignAliases(eaggs), child)
      case logical.ProjectVertices(key, vattrs, child)
          if child.resolved && vattrs.exists(_.isInstanceOf[UnresolvedAlias]) =>
        logical.ProjectVertices(key, assignAliases(vattrs), child)
    }
  }
 */

  /**
    * Pull out analytics from projections.
    */
/*
  object Analytics extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      //TODO: add edge too
      case pv @ logical.ProjectVertices(key, vattrs, child) if containsAnalytics(vattrs) =>
        logical.VertexAnalytics(pullAnalytics(vattrs), pv)
      case pe @ logical.ProjectEdges(key, eattrs, child) if containsAnalytics(eattrs) =>
        logical.EdgeAnalytics(pullAnalytics(eattrs), pe)
    }

    def containsAnalytics(exprs: Seq[Expression]): Boolean = {
      exprs.foreach(_.foreach {
        case an: SnapshotAnalytic => return true
        case _ =>
      })
      false
    }

    def pullAnalytics(exprs: Seq[NamedExpression]): Seq[NamedExpression] = {
      exprs.filter(e => e.find {
        case an: SnapshotAnalytic => true
      } != None)
    }
  }

  /**
    * Remove projections that contains aggregate expressions.
    * We don't need to turn them into anything because we already have tgroup.
    */
  object Aggregates extends Rule[LogicalPlan] {
    def apply(plan:LogicalPlan): LogicalPlan = plan resolveOperators {
      case logical.ProjectVertices(key, vattrs, child) if containsAggregates(vattrs) =>
        child
    }

    def containsAggregates(exprs: Seq[Expression]): Boolean = {
      exprs.foreach(_.foreach {
        case agg: StructuralAggregate => return true
        case _ =>
      })
      false
    }
  }
 */

/*
  object FillMissingAggregations extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case logical.TGroup(res, vsem, esem, vaggs, eaggs, child) =>
        val vschem = child.schema("V").dataType match { case st: StructType => st}
        val eschem = child.schema("E").dataType match { case st: StructType => st}
        println("V schema of child: " + vschem.map(x => x.name).mkString(","))
        //we have alias(function(attribute))
        val vmap: Map[String,UnaryExpression] = vaggs.map(expr => (expr.child.prettyName, expr)).toMap
        val emap: Map[String,UnaryExpression] = eaggs.map(expr => (expr.child.prettyName, expr)).toMap
        println(vmap.toString)
        val fullVAggs = vschem.map{ x =>
          vmap.get(x.name).getOrElse(Any(UnresolvedAttribute(Seq("V",x.name))))
        }
        val fullEAggs = eschem.map{ x =>
          emap.get(x.name).getOrElse(Any(UnresolvedAttribute(Seq("E",x.name))))
        }
        logical.TGroup(res, vsem, esem, fullVAggs, fullEAggs, child)
    }
  }
 */

}
