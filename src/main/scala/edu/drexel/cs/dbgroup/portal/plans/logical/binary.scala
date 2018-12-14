package edu.drexel.cs.dbgroup.portal.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode,LogicalPlan}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions._

import edu.drexel.cs.dbgroup.portal._

case class TemporalUnion(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output
}
case class TemporalIntersection(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output
}

