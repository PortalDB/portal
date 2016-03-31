package edu.drexel.cs.dbgroup.temporalgraph.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.{Expression,LeafExpression,Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalException

abstract class SnapshotAnalytic extends LeafExpression with Unevaluable {

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  //the children should define the datatype

}

case class Degree() extends SnapshotAnalytic {
  override def dataType: DataType = IntegerType
}

case class PageRank() extends SnapshotAnalytic {
  override def dataType: DataType = DoubleType
}
