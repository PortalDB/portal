package edu.drexel.cs.dbgroup.temporalgraph

import org.apache.spark.sql.types.{StructField,StructType,Metadata}
import org.apache.spark.sql.catalyst.expressions.{Attribute,AttributeReference}

class GraphSpec(vertexSchema: Seq[StructField], edgeSchema: Seq[StructField]) {
  def getVertexSchema(): Seq[StructField] = vertexSchema
  def getEdgeSchema(): Seq[StructField] = edgeSchema

  override def toString(): String = {
    val builder = new StringBuilder
    builder.append("V [")
    builder.append(vertexSchema.map(field => s"${field.name}:${field.dataType.simpleString}").mkString(","))
    builder.append("], E [")
    builder.append(edgeSchema.map(field => s"${field.name}:${field.dataType.simpleString}").mkString(","))
    builder.append("]")

    builder.toString()
  }

  lazy val vertexSchemaAsAttributes: Seq[Attribute] = vertexSchema.map{ v => AttributeReference(v.name, v.dataType, v.nullable, v.metadata)()}
  lazy val edgeSchemaAsAttributes: Seq[Attribute] = edgeSchema.map{ v => AttributeReference(v.name, v.dataType, v.nullable, v.metadata)()}  

  def toAttributes(): Seq[Attribute] = {
    vertexSchema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)(qualifier = Some("V"))) ++ edgeSchema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)(qualifier = Some("E")))
  }

  def validate(v: Seq[StructField], e: Seq[StructField]): Boolean = {
    v.foreach(f => if (!vertexSchema.contains(f)) return false )
    e.foreach(f => if (!edgeSchema.contains(f)) return false )
    true
  }
}

object GraphSpec {
  def apply(vertexSchema: Seq[StructField], edgeSchema: Seq[StructField]) = new GraphSpec(vertexSchema, edgeSchema)
}

class PartialGraphSpec(vertexSchema: Option[Seq[StructField]], edgeSchema: Option[Seq[StructField]]) extends GraphSpec(vertexSchema.getOrElse(Seq()), edgeSchema.getOrElse(Seq())) {
  def hasVertexSchema(): Boolean = vertexSchema != None
  def hasEdgeSchema(): Boolean = edgeSchema != None
}

object PartialGraphSpec {
  def apply(vertexSchema: Option[Seq[StructField]] = None, edgeSchema: Option[Seq[StructField]] = None) = new PartialGraphSpec(vertexSchema, edgeSchema)
}
