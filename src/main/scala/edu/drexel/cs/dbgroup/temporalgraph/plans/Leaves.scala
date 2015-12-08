package edu.drexel.cs.dbgroup.temporalgraph.plan

import java.time.LocalDate
import org.apache.spark.sql.catalyst.expressions.Attribute

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader

case class PhysicalGraph(
  output: Seq[Attribute],           //schema for this graph
  source: String,                   //location of the graph data
  start: LocalDate, end: LocalDate, //temporal bounds to load within
  graphType: String) extends LeafNode {  //the physical representation to use
  
  //TODO: incorporate partition strategy
  override def doExecute():TemporalGraphWithSchema[VertexEdgeAttribute,VertexEdgeAttribute] = {
    GraphLoader.setGraphType(graphType)
    GraphLoader.loadDataWithSchema(source, start, end, output)
  }
}

//TODO: if we start supporting subgraph on load, add as a separate case class here
