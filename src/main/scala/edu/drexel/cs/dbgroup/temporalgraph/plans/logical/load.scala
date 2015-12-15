package edu.drexel.cs.dbgroup.temporalgraph.plans.logical

import java.time.LocalDate

import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.expressions._

import edu.drexel.cs.dbgroup.temporalgraph.GraphSpec
import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader

case class LoadGraph(url: String) extends LeafNode {
  protected lazy val catalog: Seq[Attribute] = {
    GraphLoader.loadGraphDescription(url).toAttributes
  }

  override def output: Seq[Attribute] = catalog
}

case class LoadGraphWithSchema(spec: Seq[Attribute], url: String) extends LeafNode {
  //TODO: what if this is not consistent with the graph catalog?
  override def output: Seq[Attribute] = spec

}

case class LoadGraphWithDate(spec: Seq[Attribute], url: String, start: LocalDate, end: LocalDate) extends LeafNode {
  override def output: Seq[Attribute] = spec
}

case class LoadGraphFullInfo(spec: Seq[Attribute], url: String, start: LocalDate, end: LocalDate, snapAnalytics: Boolean = false, aggs: Boolean = false) extends LeafNode {
  override def output: Seq[Attribute] = spec
}
