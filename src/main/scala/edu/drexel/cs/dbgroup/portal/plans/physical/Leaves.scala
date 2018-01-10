package edu.drexel.cs.dbgroup.portal.plans.physical

import java.time.LocalDate

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.InternalRow

import edu.drexel.cs.dbgroup.portal._
import edu.drexel.cs.dbgroup.portal.util.GraphLoader
import edu.drexel.cs.dbgroup.portal.plans._

case class PhysicalGraph(
  output: Seq[Attribute],           //schema for this graph
  source: String,                   //location of the graph data
  graphType: String) extends LeafNode {  //the physical representation to use
  
  //TODO: incorporate partition strategy
  override def doExecute():TGraphWProperties = {
    GraphLoader.loadDataPropertyModel(source)
  }
}

//TODO: if we start supporting slice on load, add as a separate case class here
//TODO: if we start supporting subgraph on load, add as a separate case class here
