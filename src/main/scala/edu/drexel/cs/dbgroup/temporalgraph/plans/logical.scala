package edu.drexel.cs.dbgroup.temporalgraph.plan

import org.apache.spark.sql.catalyst.plans.logical.{LeafNode => LogicalLeafNode}
import org.apache.spark.sql.catalyst.expressions._
import edu.drexel.cs.dbgroup.temporalgraph.GraphSpec
import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader

case class LoadGraph(url: String) extends LogicalLeafNode {
  protected lazy val catalog: Seq[Attribute] = {
    GraphLoader.loadGraphDescription(url)
  }

  override def output: Seq[Attribute] = catalog
}

case class LoadGraphWithSchema(spec: GraphSpec, url: String) extends LogicalLeafNode {
  override def output: Seq[Attribute] = {
    spec.toAttributes
    //TODO: what if this is not consistent with the graph catalog?
  }
}
