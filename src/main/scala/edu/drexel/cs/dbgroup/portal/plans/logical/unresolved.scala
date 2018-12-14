package edu.drexel.cs.dbgroup.portal.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.expressions.Attribute

case class UnresolvedGraph(graphIdentifier: String) extends LeafNode {
  override def output: Seq[Attribute] = Nil
  override lazy val resolved = false
}
