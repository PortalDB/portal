package edu.drexel.cs.dbgroup.temporalgraph.expressions

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.{Expression,UnaryExpression,Unevaluable}

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
  override def dataType: DataType = child.dataType
  def name: String = child match {
    case p: Property => p.name
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
    if (value.exists(name)) {
      value.drop(name).add(name, value(name).head)
    } else value
  }
}

case class First(child: Expression) extends StructuralAggregate {
  override def initialize(initValue: VertexEdgeAttribute, interval: Interval): VertexEdgeAttribute = {
    if (initValue.exists(name)) {
      //if there are multiple values of this property, we will keep them all
      //transform each value to have the interval
      var modified = initValue.drop(name)
      initValue(name).foreach { x =>
        modified = modified.add(name, (interval, x))
      }
      modified
    } else initValue
  }
  override def process(value: VertexEdgeAttribute): VertexEdgeAttribute = {
    if (value.exists(name)){
      //pick those that have the same date which is the earliest
      implicit def ordering: Ordering[LocalDate] = TempGraphOps.dateOrdering
      val minDate = value(name).map{ case x: (Interval, scala.Any) => x._1.start}.min
      var modified = value.drop(name)
      value(name).foreach { case x: (Interval, scala.Any) =>
        if (x._1.start == minDate)
          modified = modified.add(name, x)
      }
      modified
    } else value
  }
  override def finalize(finalValue: VertexEdgeAttribute): VertexEdgeAttribute = {
    if (finalValue.exists(name)) {
      //transform each value to remove the interval
      var modified = finalValue.drop(name)
      finalValue(name).foreach { case x: (Interval, scala.Any) =>
        modified = modified.add(name, x._2)
      }
      modified
    } else finalValue
  }
}

case class Last(child: Expression) extends StructuralAggregate {
  override def initialize(initValue: VertexEdgeAttribute, interval: Interval): VertexEdgeAttribute = {
    if (initValue.exists(name)) {
      //if there are multiple values of this property, we will keep them all
      //transform each value to have the interval
      var modified = initValue.drop(name)
      initValue(name).foreach { x =>
        modified = modified.add(name, (interval, x))
      }
      modified
    } else initValue
  }
  override def process(value: VertexEdgeAttribute): VertexEdgeAttribute = {
    if (value.exists(name)){
      //pick those that have the same date which is the latest
      implicit def ordering: Ordering[LocalDate] = TempGraphOps.dateOrdering
      val maxDate = value(name).map{ case x: (Interval, scala.Any) => x._1.start}.max
      var modified = value.drop(name)
      value(name).foreach { case x: (Interval, scala.Any) =>
        if (x._1.start == maxDate)
          modified = modified.add(name, x)
      }
      modified
    } else value
  }
  override def finalize(finalValue: VertexEdgeAttribute): VertexEdgeAttribute = {
    if (finalValue.exists(name)) {
      //transform each value to remove the interval
      var modified = finalValue.drop(name)
      finalValue(name).foreach { case x: (Interval, scala.Any) =>
        modified = modified.add(name, x._2)
      }
      modified
    } else finalValue
  }
}

case class Sum(child: Expression) extends StructuralAggregate {
  override def process(value: VertexEdgeAttribute): VertexEdgeAttribute = {
    //if the property exists and is a number, we just add them up
    //but if it doesn't exist or is not a number, we leave it alone
    if (value.exists(name)) {
      var modified = value.drop(name)
      var sum = 0.0
      value(name).foreach {
        case i: Int => sum = sum + i
        case l: Long => sum = sum + l
        case d: Double => sum = sum + d
      }
      modified.add(name, sum)
    } else value
  }
}

case class Max(child: Expression) extends StructuralAggregate {
  override def process(value: VertexEdgeAttribute): VertexEdgeAttribute = {
    //if the property exists and is a number, we just pick
    //but if it doesn't exist or is not a number, we leave it alone
    if (value.exists(name)) {
      var modified = value.drop(name)
      var max = Double.MinValue
      value(name).foreach {
        case i: Int => max = math.max(max, i)
        case l: Long => max = math.max(max, l)
        case d: Double => max = math.max(max, d)
      }
      modified.add(name, max)
    } else value
  }
}
