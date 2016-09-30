package edu.drexel.cs.dbgroup.temporalgraph.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode,LogicalPlan}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions._

import edu.drexel.cs.dbgroup.temporalgraph._

case class TemporalUnion(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output
}
case class TemporalIntersection(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output
}

