package edu.drexel.cs.dbgroup.temporalgraph.plans.physical

import java.time.LocalDate

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.InternalRow

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader
import edu.drexel.cs.dbgroup.temporalgraph.plans._

case class PhysicalGraph(
  output: Seq[Attribute],           //schema for this graph
  source: String,                   //location of the graph data
  graphType: String) extends LeafNode {  //the physical representation to use
  
  //TODO: incorporate partition strategy
  override def doExecute():TGraphWProperties = {
    GraphLoader.setGraphType(graphType)
    GraphLoader.loadDataPropertyModel(source)
  }
}

//TODO: if we start supporting slice on load, add as a separate case class here
//TODO: if we start supporting subgraph on load, add as a separate case class here
