package edu.drexel.cs.dbgroup.temporalgraph.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode,LogicalPlan}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions._

import edu.drexel.cs.dbgroup.temporalgraph._

case class TemporalJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinSemantics.Value) extends BinaryNode {

  //empty sequence for aggregations means "any"
  var vertexAggregations: Seq[Expression] = Seq()
  var edgeAggregations: Seq[Expression] = Seq()

  override def output: Seq[Attribute] = left.output
}
