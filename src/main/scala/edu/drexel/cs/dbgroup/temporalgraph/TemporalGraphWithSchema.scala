package edu.drexel.cs.dbgroup.temporalgraph

import scala.reflect.ClassTag
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.graphx.{Edge,VertexId}

trait TemporalGraphWithSchema extends TemporalGraph[InternalRow,InternalRow] {

  def getSchema(): GraphSpec

  def projectVertices(input: Seq[NamedExpression]): TemporalGraphWithSchema
  def projectEdges(input: Seq[NamedExpression]): TemporalGraphWithSchema

  //TODO: add the others as necessary
  //FIXME: figure out a way to not have to list all methods inherited from
  //parent and still return this type (use member type?)
  def mapVerticesWIndex(map: (VertexId, TimeIndex, InternalRow) => InternalRow, newSchema: GraphSpec): TemporalGraphWithSchema
  def mapEdgesWIndex(map: (Edge[InternalRow], TimeIndex) => InternalRow, newSchema: GraphSpec): TemporalGraphWithSchema
  def aggregate(res: Resolution, vsem: AggregateSemantics.Value, esem: AggregateSemantics.Value, vAggFunc: (InternalRow, InternalRow) => InternalRow, eAggFunc: (InternalRow, InternalRow) => InternalRow): TemporalGraphWithSchema
}
