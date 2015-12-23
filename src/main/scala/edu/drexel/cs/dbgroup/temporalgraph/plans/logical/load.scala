package edu.drexel.cs.dbgroup.temporalgraph.plans.logical

import java.time.LocalDate

import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StructField

import edu.drexel.cs.dbgroup.temporalgraph.{GraphSpec,PartialGraphSpec}
import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader

case class LoadGraph(url: String, start: LocalDate, end: LocalDate) extends LeafNode {
  protected lazy val catalog: Seq[Attribute] = {
    GraphLoader.loadGraphDescription(url).toAttributes
  }

  override def output: Seq[Attribute] = catalog
}

case class LoadGraphWithSchema(sp: PartialGraphSpec, url: String, start: LocalDate, end: LocalDate) extends LeafNode {
  //load the attributes if missing, also check correctness
  private val fullSpec: GraphSpec = GraphLoader.loadGraphDescription(url)
  private val vFields: Seq[StructField] = if (sp.hasVertexSchema()) sp.getVertexSchema() else fullSpec.getVertexSchema
  private val eFields: Seq[StructField] = if (sp.hasEdgeSchema()) sp.getEdgeSchema() else fullSpec.getEdgeSchema
  val spec: GraphSpec = GraphSpec(vFields, eFields)
  if (!fullSpec.validate(vFields, eFields))
    throw new IllegalArgumentException("Invalid graph schema requested. Valid fields: " + fullSpec.toString)

  override def output: Seq[Attribute] = spec.toAttributes()
}

case class LoadGraphFullInfo(spec: Seq[Attribute], url: String, start: LocalDate, end: LocalDate, snapAnalytics: Boolean = false, aggs: Boolean = false) extends LeafNode {
  override def output: Seq[Attribute] = spec
}
