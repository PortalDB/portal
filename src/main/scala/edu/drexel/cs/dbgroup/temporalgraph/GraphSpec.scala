package edu.drexel.cs.dbgroup.temporalgraph

import org.apache.spark.sql.types.{StructField,StructType,Metadata}
import org.apache.spark.sql.catalyst.expressions.{Attribute,AttributeReference}

class GraphSpec(vertexSchema: Seq[StructField], edgeSchema: Seq[StructField]) {
  def getVertexSchema(): Seq[StructField] = vertexSchema
  def getEdgeSchema(): Seq[StructField] = edgeSchema

  override def toString(): String = {
    val builder = new StringBuilder
    builder.append("V [")
    builder.append(vertexSchema.map(field => s"${field.name}:${field.dataType.simpleString}"))
    builder.append("], E [")
    builder.append(edgeSchema.map(field => s"${field.name}:${field.dataType.simpleString}"))
    builder.append("]")

    builder.toString()
  }

  lazy val vertexSchemaAsAttributes: Seq[Attribute] = vertexSchema.map{ v => AttributeReference(v.name, v.dataType, v.nullable, v.metadata)()}
  lazy val edgeSchemaAsAttributes: Seq[Attribute] = edgeSchema.map{ v => AttributeReference(v.name, v.dataType, v.nullable, v.metadata)()}  

  def toAttributes(): Seq[Attribute] = {
    Seq(AttributeReference("V", StructType(vertexSchema), false, Metadata.empty)(),
      AttributeReference("E", StructType(edgeSchema), false, Metadata.empty)())
  }

  //takes in a full(er) schema and returns two bitsets for which fields (by index)
  //this one has
  def getIndices(fullSchema: GraphSpec): (Array[Boolean], Array[Boolean]) = {
    var tmp: Int = 0
    val fullVertexSchema = fullSchema.getVertexSchema
    val fullEdgeSchema = fullSchema.getEdgeSchema
    val vIndices = (0 until fullSchema.getVertexSchema.length).map { index =>
      if (vertexSchema.length > tmp && vertexSchema(tmp).equals(fullVertexSchema(index))) {
        tmp += 1
        true
      } else false
    }.toArray
    tmp = 0
    val eIndices = (0 until fullSchema.getEdgeSchema.length).map { index =>
      if (edgeSchema.length > tmp && edgeSchema(tmp).equals(fullEdgeSchema(index))) {
        tmp += 1
        true
      } else false
    }.toArray
    (vIndices, eIndices)
  }
}

object GraphSpec {
  def apply(vertexSchema: Seq[StructField], edgeSchema: Seq[StructField]) = new GraphSpec(vertexSchema, edgeSchema)
}
