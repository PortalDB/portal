package edu.drexel.cs.dbgroup.temporalgraph.plan

import org.apache.spark.sql.catalyst.plans.logical.{LeafNode => LogicalLeafNode}
import org.apache.spark.sql.catalyst.expressions._
import edu.drexel.cs.dbgroup.temporalgraph.GraphSpec

/**
  * For all graphs, the 

*/

case class LoadGraph(url: String) extends LogicalLeafNode {
  override def output: Seq[Attribute] = {
    Seq.empty
    //TODO: it is whatever the catalog says it is
    //do we read this from the file system?
  }
}

case class LoadGraphWithSchema(spec: GraphSpec, url: String) extends LogicalLeafNode {
  override def output: Seq[Attribute] = {
    Seq.empty
    //TODO: it is whatever the catalog says it is
    //do we read this from the file system?
  }
}
