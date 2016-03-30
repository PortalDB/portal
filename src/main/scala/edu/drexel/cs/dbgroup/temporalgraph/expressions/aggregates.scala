package edu.drexel.cs.dbgroup.temporalgraph.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.{Expression,UnaryExpression,Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalException

//while these functions are not removed during optimization,
//they are still considered unevaluable because they are manually
//applied to individual fields by the aggregate operator
abstract class StructuralAggregate extends UnaryExpression with Unevaluable {

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  //children should override if changing type
  override def dataType: DataType = child.dataType

  /** the processing of these aggregate functions proceeds in three steps:
   * 1. initialize the vertex/edge value; can be noop
   * 2. process during aggregation by combining two values at a time
   * 3. perform the final transform on the final value; can be noop
   * Look at examples below for how to define complex aggregates
   */
  //TODO: can we change to ClassTag here instead of Any for better type checking?
  def initialize(initValue: scala.Any, index: TimeIndex): scala.Any = initValue
  def process(value1: scala.Any, value2: scala.Any): scala.Any
  def finalize(finalValue: scala.Any): scala.Any = finalValue
  def processDataType: DataType = child.dataType
}

case class Any(child: Expression) extends StructuralAggregate {
  override def process(value1: scala.Any, value2: scala.Any) = value1
}

case class First(child: Expression) extends StructuralAggregate {
  override def initialize(initValue: scala.Any, index: TimeIndex): scala.Any = Map(index -> initValue)
  override def processDataType: DataType = MapType(IntegerType, child.dataType, false)
  override def process(value1: scala.Any, value2: scala.Any): scala.Any = {
    if (value1.asInstanceOf[Map[TimeIndex,scala.Any]].head._1 < value2.asInstanceOf[Map[TimeIndex,scala.Any]].head._1) value1 else value2
  }
  override def finalize(finalValue: scala.Any): scala.Any = finalValue.asInstanceOf[Map[TimeIndex,scala.Any]].head._2
}

case class Last(child: Expression) extends StructuralAggregate {
  override def initialize(initValue: scala.Any, index: TimeIndex): scala.Any = Tuple2(index, initValue)
  override def processDataType: DataType = MapType(IntegerType, child.dataType, false)
  override def process(value1: scala.Any, value2: scala.Any): scala.Any = if (value1.asInstanceOf[Map[TimeIndex,scala.Any]].head._1 > value2.asInstanceOf[Map[TimeIndex,scala.Any]].head._1) value1 else value2
  override def finalize(finalValue: scala.Any): scala.Any = finalValue.asInstanceOf[Map[TimeIndex,scala.Any]].head._2
}

case class Sum(child: Expression) extends StructuralAggregate {
  override def process(value1: scala.Any, value2: scala.Any): scala.Any = {
    //TODO: we should be able to use more generic NumericType instead of
    //listing each one separately
    dataType match {
      case it: IntegerType => value1.asInstanceOf[Integer] + value2.asInstanceOf[Integer]
      case lt: LongType => value1.asInstanceOf[Long] + value2.asInstanceOf[Long]
      case st: StringType => value1.asInstanceOf[String] + value2.asInstanceOf[String]
      case _ => throw new PortalException("cannot sum non-numeric types")
    }
  }
}
