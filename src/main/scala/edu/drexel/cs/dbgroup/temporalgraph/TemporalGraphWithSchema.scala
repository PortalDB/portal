package edu.drexel.cs.dbgroup.temporalgraph

import scala.reflect.ClassTag
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.InternalRow

trait TemporalGraphWithSchema extends TemporalGraph[InternalRow,InternalRow] {
  def getSchema(): GraphSpec

  def projectVertices(input: Seq[NamedExpression]): TemporalGraphWithSchema
  def projectEdges(input: Seq[NamedExpression]): TemporalGraphWithSchema
}
