package edu.drexel.cs.dbgroup.portal.plans.logical

import java.time.LocalDate

import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode,LogicalPlan}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.types.{StructType,Metadata,StructField}

import edu.drexel.cs.dbgroup.portal._
import edu.drexel.cs.dbgroup.portal.portal.PortalException
import edu.drexel.cs.dbgroup.portal.expressions._

case class Slice(start: LocalDate, end: LocalDate, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}
case class Subgraph(vertexCondition: Expression, edgeCondition: Expression, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}
//TODO: this logic should reside in the analyzer, not here
case class VertexMap(expr: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = catalog
  lazy val catalog = {
    //keep all the E attributes
    if (expr.find(_.isInstanceOf[PropertyStar]).isEmpty)
      expr.map(_.toAttribute.withQualifier(Some("V"))) ++ child.output.filter(f => f.qualifier == Some("E"))
    else {
      //need to include all properties that aren't listed in expr from child output
      val attrs = expr.flatMap(e => e.find {
        case at: Attribute => true
        case _ => false
      }).asInstanceOf[Seq[NamedExpression]].map(e => e.name)
      expr.flatMap{
        case ps: PropertyStar => None
        case other => Some(other.toAttribute.withQualifier(Some("V")))
      } ++ child.output.filter(f => f.qualifier == Some("E") || (f.qualifier == Some("V") && attrs.collectFirst{ case s: String if s == f.name => s}.isEmpty))
    }
  }
}
case class EdgeMap(expr: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = catalog
  lazy val catalog = {
    //keep all the V attributes
    if (expr.find(_.isInstanceOf[PropertyStar]).isEmpty)
      child.output.filter(f => f.qualifier == Some("V")) ++ expr.map(_.toAttribute.withQualifier(Some("E")))
    else {
      //need to include all properties that aren't listed in expr from child output
      val attrs = expr.flatMap(e => e.find {
        case at: Attribute => true
        case _ => false
      }).asInstanceOf[Seq[NamedExpression]].map(e => e.name)
      expr.flatMap{
        case ps: PropertyStar => None
        case other => Some(other.toAttribute.withQualifier(Some("E")))
      } ++ child.output.filter(f => f.qualifier == Some("V") || (f.qualifier == Some("E") && attrs.collectFirst{ case s: String if s == f.name => s}.isEmpty))
    }
  }
}

case class Aggregate(window: WindowSpecification, vertexQuant: Quantification, edgeQuant: Quantification, vertexAggregations: Seq[NamedExpression], edgeAggregations: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = catalog //child.output 
  lazy val catalog = {
    //we keep all properties, but for those explicitly specified there's an alias
    //which changes the exprid and potentially the name
    val vs = vertexAggregations.map(_.toAttribute).map(e => e.withQualifier(Some("V")))
    val es = edgeAggregations.map(_.toAttribute).map(e => e.withQualifier(Some("E")))
    val vnames = vertexAggregations.flatMap {
      case al @ Alias(child, name) if child.isInstanceOf[StructuralAggregate] =>
        child.asInstanceOf[StructuralAggregate].child match {
          case at: Attribute => Some(at.name)
          case _ => None
        }
      case _ => None
    }
    val enames = edgeAggregations.flatMap {
      case al @ Alias(child, name) if child.isInstanceOf[StructuralAggregate] =>
        child.asInstanceOf[StructuralAggregate].child match {
          case at: Attribute => Some(at.name)
          case _ => None
        }
      case _ => None
    }

    child.output.filter{ case f => 
      f.qualifier == Some("V") && vnames.collectFirst { case s: String if s == f.name => s}.isEmpty
    } ++ vs ++ child.output.filter{ case f =>
        f.qualifier == Some("E") && enames.collectFirst { case s: String if s == f.name => s}.isEmpty
    } ++ es
  }
}

case class VertexAnalytics(vertexAnalytics: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = catalog
  lazy val catalog = {
    //include all children's fields plus any analytics produced here
    child.output ++ vertexAnalytics.map(_.toAttribute.withQualifier(Some("V")))
  }
}
