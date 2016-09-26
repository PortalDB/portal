package edu.drexel.cs.dbgroup.temporalgraph.plans.logical

import java.time.LocalDate

import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode,LogicalPlan}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalException

case class Slice(start: LocalDate, end: LocalDate, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}
case class Subgraph(vertexCondition: Expression, edgeCondition: Expression, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}
case class VertexMap(expr: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}
case class EdgeMap(expr: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class Aggregate(window: WindowSpecification, vertexQuant: Quantification, edgeQuant: Quantification, vertexAggregations: Seq[NamedExpression], edgeAggregations: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output 
}

case class VertexAnalytics(vertexAnalytics: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}
