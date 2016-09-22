package edu.drexel.cs.dbgroup.temporalgraph.plans.physical

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.graphx.{Edge,VertexId}
import org.apache.spark.sql.types.{StructType,Metadata,StructField}
import org.apache.spark.sql.catalyst.expressions.{Attribute,AttributeReference,NamedExpression}

import edu.drexel.cs.dbgroup.temporalgraph.expressions.StructuralAggregate
import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.plans._

case class TGroup(
  resolution: Resolution, 
  vertexSemantics: AggregateSemantics.Value, 
  edgeSemantics: AggregateSemantics.Value, 
  vertexAggregations: Seq[NamedExpression], //StructuralAggregate], 
  edgeAggregations: Seq[NamedExpression], //StructuralAggregate], 
  child: PortalPlan) extends UnaryNode {

  //the number of aggregations has to match the child's number of attributes
  if (child.vertexSchema.length != vertexAggregations.size || child.edgeSchema.length != edgeAggregations.size)
    throw new IllegalStateException("the number of structural aggregations should have been matched to the target")

  private lazy val vSchema = vertexAggregates.map(x => x.processDataType)
  private lazy val eSchema = edgeAggregates.map(x => x.processDataType)
  val vatrs: Seq[StructField] = vertexAggregations.map(f => StructField(f.name, f.dataType, f.nullable, Metadata.empty))
  val eatrs: Seq[StructField] = edgeAggregations.map(f => StructField(f.name, f.dataType, f.nullable, Metadata.empty))

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("V", StructType(vatrs), false, Metadata.empty)(), AttributeReference("E", StructType(eatrs), false, Metadata.empty)())    
  }

  val vertexAggregates = vertexAggregations.flatMap { agg =>
    agg.collect {
      case a: StructuralAggregate => a
    }
  }.toArray
  val edgeAggregates = edgeAggregations.flatMap { agg =>
    agg.collect {
      case a: StructuralAggregate => a
    }
  }.toArray

  override def doExecute(): TemporalGraphWithSchema = {
    def vInitFunc(vid: VertexId, index: TimeIndex, v: InternalRow): InternalRow = {
      val outputArray = new Array[Any](vertexAggregates.length)
      val vschem = child.schema("V").dataType match {
        case StructType(tp) => tp.toSeq
      }
      val inputArray = v.toSeq(vschem.map(x => x.dataType))
      var i = 0
      while (i < vertexAggregates.length) {
        outputArray(i) = vertexAggregates(i).initialize(inputArray(i), index)
        i += 1
      }
      new GenericInternalRow(outputArray)
    }
    def eInitFunc(e: Edge[InternalRow], index: TimeIndex): InternalRow = {
      val outputArray = new Array[Any](edgeAggregates.length)
      val eschem = child.schema("E").dataType match {
        case StructType(tp) => tp.toSeq
      }
      val inputArray = e.attr.toSeq(eschem.map(x => x.dataType))
      var i = 0
      while (i < edgeAggregates.length) {
        outputArray(i) = edgeAggregates(i).initialize(inputArray(i), index)
        i += 1
      }
      new GenericInternalRow(outputArray)
    }

    def vAggFunc(v1: InternalRow, v2: InternalRow): InternalRow = {
      val outputArray = new Array[Any](vertexAggregates.length)
      val input1Array = v1.toSeq(vSchema)
      val input2Array = v2.toSeq(vSchema)
      var i = 0
      while (i < vertexAggregates.length) {
        outputArray(i) = vertexAggregates(i).process(input1Array(i), input2Array(i))
        i += 1
      }
      new GenericInternalRow(outputArray)
    }
    def eAggFunc(e1: InternalRow, e2: InternalRow): InternalRow = {
      val outputArray = new Array[Any](edgeAggregates.length)
      val input1Array = e1.toSeq(eSchema)
      val input2Array = e2.toSeq(eSchema)
      var i = 0
      while (i < edgeAggregates.length) {
        outputArray(i) = edgeAggregates(i).process(input1Array(i), input2Array(i))
        i += 1
      }
      new GenericInternalRow(outputArray)
    }

    def vFinalFunc(vid: VertexId, index: TimeIndex, v: InternalRow): InternalRow = {
      val outputArray = new Array[Any](vertexAggregates.length)
      val inputArray = v.toSeq(vSchema)
      var i = 0
      while (i < vertexAggregates.length) {
        outputArray(i) = vertexAggregates(i).finalize(inputArray(i))
        i += 1
      }
      new GenericInternalRow(outputArray)
    }
    def eFinalFunc(e: Edge[InternalRow], index: TimeIndex): InternalRow = {
      val outputArray = new Array[Any](edgeAggregates.length)
      val inputArray = e.attr.toSeq(eSchema)
      var i = 0
      while (i < edgeAggregates.length) {
        outputArray(i) = edgeAggregates(i).finalize(inputArray(i))
        i += 1
      }
      new GenericInternalRow(outputArray)
    }

    var res: TemporalGraphWithSchema = child.execute()
    res.mapVerticesWIndex(vInitFunc, res.getSchema)
      .mapEdgesWIndex(eInitFunc, res.getSchema)
      .aggregate(resolution, vertexSemantics, edgeSemantics, vAggFunc, eAggFunc)
      .mapVerticesWIndex(vFinalFunc, res.getSchema)
      .mapEdgesWIndex(eFinalFunc, GraphSpec(vatrs, eatrs))
  }

}
