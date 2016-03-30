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
  start: LocalDate, end: LocalDate, //temporal bounds to load within
  graphType: String) extends LeafNode {  //the physical representation to use
  
  //TODO: incorporate partition strategy
  override def doExecute():TemporalGraphWithSchema = {
    GraphLoader.setGraphType(graphType)
    val vertexFields = schema("V").dataType match {
      case StructType(tp) => tp.toSeq
    }
    val edgeFields = schema("E").dataType match {
      case StructType(tp) => tp.toSeq
    }
    val spec: GraphSpec = new GraphSpec(vertexFields, edgeFields)
    GraphLoader.loadDataWithSchema(source, start, end, spec)
  }
}

//TODO: if we start supporting subgraph on load, add as a separate case class here
