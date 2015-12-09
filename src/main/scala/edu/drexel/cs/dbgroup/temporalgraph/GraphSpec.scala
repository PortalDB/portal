package edu.drexel.cs.dbgroup.temporalgraph

import org.apache.spark.sql.types.StructField

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
}
