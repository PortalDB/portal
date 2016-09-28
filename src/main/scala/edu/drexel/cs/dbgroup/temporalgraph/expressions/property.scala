package edu.drexel.cs.dbgroup.temporalgraph.expressions

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{LongType,DataType}
import org.apache.spark.sql.types.Metadata

case class Property(name: String)(val exprId: ExprId = NamedExpression.newExprId) extends LeafExpression with NamedExpression with Unevaluable {
  override def qualifier: Option[String] = None
  override def toAttribute: Attribute = AttributeReference(name, dataType, nullable, Metadata.empty)(exprId, qualifier, isGenerated)
  //this is a bit of a hack since properties can be of whatever type and we don't care
  override def dataType: DataType = LongType
  override def nullable: Boolean = true
  override def newInstance(): NamedExpression = Property(name)()
  override lazy val resolved = true

}

case class PropertyStar()(val exprId: ExprId = NamedExpression.newExprId) extends LeafExpression with NamedExpression with Unevaluable {
  override def name: String = "PropertyStar"
  override def qualifier: Option[String] = None
  override def toAttribute: Attribute = AttributeReference(name, dataType, nullable, Metadata.empty)(exprId, qualifier, isGenerated)
  //this is a bit of a hack since properties can be of whatever type and we don't care
  override def dataType: DataType = LongType
  override def nullable: Boolean = true
  override def newInstance(): NamedExpression = Property(name)()
  override lazy val resolved = true

}
