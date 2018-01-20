package edu.drexel.cs.dbgroup.portal.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.{Expression,LeafExpression,Unevaluable}

import edu.drexel.cs.dbgroup.portal._
import edu.drexel.cs.dbgroup.portal.portal.PortalException

abstract class SnapshotAnalytic extends LeafExpression with Unevaluable {

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  //the children should define the datatype
  val propertyName: String = ""
}

case class Degree() extends SnapshotAnalytic {
  override def dataType: DataType = IntegerType
  override val propertyName: String = "degree"
}

case class PageRank() extends SnapshotAnalytic {
  override def dataType: DataType = DoubleType
  override val propertyName: String = "pagerank"
}

case class ConnectedComponents() extends SnapshotAnalytic {
  override def dataType: DataType = LongType
  override val propertyName: String = "components"
}
