package edu.drexel.cs.dbgroup.temporalgraph.expressions

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.{Expression,UnaryExpression,Unevaluable,Attribute}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalException
import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps

import java.time.LocalDate

//while these functions are not removed during optimization,
//they are still considered unevaluable because they are manually
//applied to individual fields by the aggregate operator
abstract class StructuralAggregate extends UnaryExpression with Unevaluable {

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  //children should override as necessary
  override def dataType: DataType = child.dataType
  def name: String = child match {
    case p: Attribute => p.name
    case _ => throw new PortalException("cannot define an aggregate over non-property expression")
  }

  /** the processing of these aggregate functions proceeds in three steps:
   * 1. initialize the vertex/edge value; can be noop
   * 2. process during aggregation from one or more values in a bag
   * 3. perform the final transform on the final value; can be noop
   * Look at examples below for how to define complex aggregates
   */
  def initialize(initValue: VertexEdgeAttribute, interval: Interval): VertexEdgeAttribute = initValue
  def process(value: VertexEdgeAttribute): VertexEdgeAttribute
  def finalize(finalValue: VertexEdgeAttribute): VertexEdgeAttribute = finalValue
}

case class Any(child: Expression) extends StructuralAggregate {
  override def process(value: VertexEdgeAttribute) = {
    value
  }
}

case class First(child: Expression) extends StructuralAggregate {
  override def process(value: VertexEdgeAttribute): VertexEdgeAttribute = {
    //fixme
    value
  }
}

case class Last(child: Expression) extends StructuralAggregate {
  override def process(value: VertexEdgeAttribute): VertexEdgeAttribute = {
    //fixme
    value
  }
}

case class Sum(child: Expression) extends StructuralAggregate {
  override def process(value: VertexEdgeAttribute): VertexEdgeAttribute = {
    //fixme
    value
  }
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function sum")
}

case class Max(child: Expression) extends StructuralAggregate {
  override def process(value: VertexEdgeAttribute): VertexEdgeAttribute = {
    //fixme
    value
  }
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function max")
}
