package edu.drexel.cs.dbgroup.temporalgraph

import org.apache.spark.sql.types.{StructField,StructType,Metadata}
import org.apache.spark.sql.catalyst.expressions.{Attribute,AttributeReference}

class GraphSpec(vertexSchema: Seq[StructField], edgeSchema: Seq[StructField]) {
  override def toString(): String = {
    val builder = new StringBuilder
    builder.append("V [")
    builder.append(vertexSchema.map(field => s"${field.name}:${field.dataType.simpleString}"))
    builder.append("], E [")
    builder.append(edgeSchema.map(field => s"${field.name}:${field.dataType.simpleString}"))
    builder.append("]")

    builder.toString()
  }

  def toAttributes(): Seq[Attribute] = {
    Seq(AttributeReference("V", StructType(vertexSchema), false, Metadata.empty)(),
      AttributeReference("E", StructType(edgeSchema), false, Metadata.empty)())
  }
}
