package edu.drexel.cs.dbgroup.portal.portal

import scala.collection.immutable
import java.time.LocalDate

import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.{CatalystConf,TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{StructType,StructField}
import org.apache.spark.sql.catalyst.expressions._

import org.apache.spark.sql.ModifierWorkaround

import edu.drexel.cs.dbgroup.portal.plans._
import edu.drexel.cs.dbgroup.portal.expressions._

class PortalAnalyzer(catalog: SessionCatalog, registry: FunctionRegistry, conf: CatalystConf) extends Analyzer(catalog, conf) {

  override lazy val batches: Seq[Batch] = Seq(
    Batch("Substitution", fixedPoint,
      Slice),
    Batch("Resolution", fixedPoint,
      ResolveGraphs ::
        ResolvePortalReferences ::
        ResolvePortalFunctions ::
        ResolvePortalAliases ::
        Aggregates ::
        Analytics ::
      Nil : _*),
    Batch("Cleanup", Once,
      CleanupMaps)
  )

  override val extendedCheckRules: Seq[LogicalPlan => Unit] = Seq(
  )

  object ResolveGraphs extends Rule[LogicalPlan] {
    def getGraph(u: logical.UnresolvedGraph): LogicalPlan = {
      try {
        EliminateSubqueryAliases(catalog.lookupRelation(ModifierWorkaround.makeTableIdentifier(u.graphIdentifier)))
      } catch {
        case _: NoSuchTableException =>
          failAnalysis("no such view ${u.graphIdentifier}")
      }
    }
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case u: logical.UnresolvedGraph => getGraph(u)
    }
  }

  /**
    * We have no schema, so we need to replace all unresolvedattributes
    * with Properties
    */
  object ResolvePortalReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p: LogicalPlan if !p.childrenResolved => p

      case q: LogicalPlan if !q.resolved =>
        q transformExpressionsUp  {
          case u @ UnresolvedAttribute(nameParts) =>
            // Leave unchanged if resolution fails.  Hopefully will be resolved next round.
            val result =
              withPosition(u) { q.resolveChildren(nameParts, resolver).getOrElse(u) }
            result
        }
    }
  }

  /**
    * Pull slice conditions from expressions
    */
  object Slice extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case vw @ logical.Subgraph(v, e, child) if containsTimeConditions(v) =>
        val (st, en) = extractDates(v)
        val newv = pullTimeConditions(v)
        val newe = pullTimeConditions(e)
        if (newv.resolved && newv.foldable && newe.resolved && newe.foldable) //TODO: is this the right test?
          logical.Slice(st, en, child)
        else
          logical.Subgraph(pullTimeConditions(v), pullTimeConditions(e), logical.Slice(st, en, child))
    }

    //TODO: this hard-coded comparison is dirty, replace with keyword
    def containsTimeConditions(exp: Expression): Boolean = {
      exp.collectFirst {
        case u: UnresolvedAttribute if (u.name.toLowerCase == "start" || u.name.toLowerCase == "end") => u
      }.isDefined
    }

    private def isStart(exp: Expression): Boolean = {
      exp match {
        case u: UnresolvedAttribute =>
          u.name.toLowerCase == "start"
        case _ => false
      }
    }
    private def isEnd(exp: Expression): Boolean = {
      exp match {
        case u: UnresolvedAttribute =>
          u.name.toLowerCase == "end"
        case _ => false
      }
    }

    //FIXME: this only works for one case where the left side is a 'start' or 'end'
    //and doesn't check the operators which have to be
    //>= for start and < for end to be correct
    //rewrite better
    def extractDates(vw: Expression): (LocalDate, LocalDate) = {
      val extracted: Seq[(String, LocalDate)] = vw.collect {
        case bcs: BinaryComparison if isStart(bcs.left) =>
          ("start", LocalDate.parse(bcs.right.toString))
        case bce: BinaryComparison if isEnd(bce.left) =>
          ("end", LocalDate.parse(bce.right.toString))
      }
      val emap = extracted.toMap
      val st = emap.getOrElse("start", LocalDate.MIN)
      val en = emap.getOrElse("end", LocalDate.MAX)
      (st, en)
    }

    def pullTimeConditions(exp: Expression): Expression = {
      exp transform {
        case bc: BinaryComparison if (isStart(bc.left) || isEnd(bc.left)) => Literal(true)
      }
    }
  }

  /**
   * Replaces [[UnresolvedFunction]]s with concrete [[Expression]]s.
   */
  object ResolvePortalFunctions extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case q: LogicalPlan =>
        q transformExpressions {
          case u if !u.childrenResolved => 
            u // Skip until children are resolved.
          case u @ UnresolvedFunction(name, children, isDistinct) =>
            //println("trying to resolve function " + name + " in plan " + q)
            val res = withPosition(u) {
              catalog.lookupFunction(name, children) match {
                case other => 
                  other
              }
            }
            //println("result: " + res)
            res
        }
    }
  }

  /**
    * Replaces [[UnresolvedAlias]]s with concrete aliases.
    * We do not allow anonymous fields.
  */
  object ResolvePortalAliases extends Rule[LogicalPlan] {
    private def assignAliases(exprs: Seq[NamedExpression]): Seq[NamedExpression] = {
      // The `UnresolvedAlias`s will appear only at root of a expression tree, we don't need
      // to traverse the whole tree.
      exprs.zipWithIndex.map {
        case (u @ UnresolvedAlias(child, optGenAliasFunc), i) =>
          child match {
            case _: UnresolvedAttribute => u
            case a: SnapshotAnalytic =>
              if (optGenAliasFunc.isDefined)
                Alias(a, optGenAliasFunc.get.apply(a))()
              else
                Alias(a, a.propertyName)()
            case s: StructuralAggregate =>
              if (optGenAliasFunc.isDefined)
                Alias(s, optGenAliasFunc.get.apply(s))()
              else
                Alias(s, s.name)()
            case ne: NamedExpression => ne
            case e if !e.resolved => u
            case other => 
              Alias(other, s"_c$i")()
          }
        case (other, _) => other
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case logical.VertexMap(vattrs, child)
          if child.resolved && vattrs.exists(_.isInstanceOf[UnresolvedAlias]) =>
        logical.VertexMap(assignAliases(vattrs), child)
      case logical.EdgeMap(eattrs, child)
          if child.resolved && eattrs.exists(_.isInstanceOf[UnresolvedAlias]) =>
        logical.EdgeMap(assignAliases(eattrs), child)
      case logical.Aggregate(w, vq, eq, va, ea, child)
          if child.resolved && (va.exists(_.isInstanceOf[UnresolvedAlias]) || ea.exists(_.isInstanceOf[UnresolvedAlias])) =>
        logical.Aggregate(w, vq, eq, assignAliases(va), assignAliases(ea), child)
    }
  }

  /**
    * Pull out analytics from projections.
    */
  object Analytics extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      //TODO: add edge too if we have edge analytics
      case pv @ logical.VertexMap(vattrs, child) if containsAnalytics(vattrs) =>
        logical.VertexMap(removeAnalytics(vattrs), logical.VertexAnalytics(pullAnalytics(vattrs), child))
    }

    def containsAnalytics(exprs: Seq[Expression]): Boolean = {
      exprs.foreach(_.foreach {
        case an: SnapshotAnalytic => return true
        case _ =>
      })
      false
    }

    def removeAnalytics(exprs: Seq[NamedExpression]): Seq[NamedExpression] = {
      exprs.map { e => e.transformUp {
        case al: Alias =>
          al.child match {
            case an: SnapshotAnalytic => AttributeReference(al.name, an.dataType, true)(al.exprId, Some("V"), false)
            case other => al
          }
      }
      }.asInstanceOf[Seq[NamedExpression]]
    }

    def pullAnalytics(exprs: Seq[NamedExpression]): Seq[NamedExpression] = {
      exprs.filter(e => e.find {
        case al: Alias =>
          al.child match {
            case an: SnapshotAnalytic => true
            case _ => false
          }
        case _ => false
      } != None)
    }
  }

  /**
    * Remove projections that contains aggregate expressions.
    * We don't need to turn them into anything because we already have tgroup.
    * However, structural aggregates can be used in a map as well,
    * so only remove them if there is already an aggregation below.
    */
  object Aggregates extends Rule[LogicalPlan] {
    def apply(plan:LogicalPlan): LogicalPlan = plan resolveOperators {
      case q: LogicalPlan if !q.childrenResolved => q
      case vm @ logical.VertexMap(vattrs, child) if containsAggregates(vattrs) =>
        val resolved = vattrs.map(resolveAggregateExpression(_, child)).asInstanceOf[Seq[NamedExpression]]
        if (resolved == vattrs)
          vm
        else
          logical.VertexMap(resolved, child)
      case em @ logical.EdgeMap(eattrs, child) if containsAggregates(eattrs) =>
        //aggregation is likely not the immediate child, so have to look for it
        val resolved = eattrs.map(resolveAggregateExpression(_, child)).asInstanceOf[Seq[NamedExpression]]
        if (resolved == eattrs)
          em
        else
          logical.EdgeMap(resolved, child)
      case ag @ logical.Aggregate(w, vq, eq, va, ea, child) if containsNonAggregates(va) || containsNonAggregates(ea) =>
        logical.Aggregate(w, vq, eq, removeNonAggregates(va), removeNonAggregates(ea), child)
    }

    def resolveAggregateExpression(
      expr: Expression,
      plan: LogicalPlan) = {
    try {
      expr transformUp {
        case u @ UnresolvedAttribute(nameParts) =>
          withPosition(u) { plan.resolve(nameParts, resolver).getOrElse(u) }
        case al @ Alias(child, name) =>
          withPosition(al) { plan.resolve(Seq(name), resolver).getOrElse(al) }
        case st: StructuralAggregate =>
          withPosition(st) { plan.resolve(Seq(st.name), resolver).getOrElse(st) }
      }
    } catch {
      case a: Exception => expr
    }
  }

    def containsAggregates(exprs: Seq[Expression]): Boolean = {
      exprs.foreach(_.foreach {
        case agg: StructuralAggregate => return true
        case un: UnresolvedFunction => return true
        case _ =>
      })
      false
    }

    def containsNonAggregates(exprs: Seq[NamedExpression]): Boolean = {
      exprs.collectFirst {
        case al @ Alias(child, name) if child.isInstanceOf[StructuralAggregate] => 
        case other => other
      }.isDefined
    }

    def removeNonAggregates(exprs: Seq[NamedExpression]): Seq[NamedExpression] = {
      exprs.flatMap(e => e.find {
        case al @ Alias(child: StructuralAggregate, name) => true
        case agg: StructuralAggregate => true
        case _ => false
      }).map{
        case agg: StructuralAggregate => Alias(agg, agg.name)()
        case other => other
      }.asInstanceOf[Seq[NamedExpression]]
 
    }

  }

  /**
    * Remove maps that don't do anything, left over from analytics and aggregates
    */
  object CleanupMaps extends Rule[LogicalPlan] {
    def containsProjections(attrs: Seq[NamedExpression]): Boolean = {
      if (attrs.isEmpty) return false
      //if we only have regular properties listed with no expressions
      //and a star as well, then we have no projection/map
      attrs.foreach(_.foreach {
        case p: AttributeReference =>
        case ps: PropertyStar =>
        case other => return true
      })
      attrs.foreach(_.foreach {
        case al: PropertyStar => return false
        case _ =>
      })
      true
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case logical.VertexMap(vatrs, child) if !containsProjections(vatrs) => child
      case logical.EdgeMap(eatrs, child) if !containsProjections(eatrs) => child
    }
  }

}
