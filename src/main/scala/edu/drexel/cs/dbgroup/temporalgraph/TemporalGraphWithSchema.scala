package edu.drexel.cs.dbgroup.temporalgraph

import scala.reflect.ClassTag
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions.NamedExpression

abstract class TemporalGraphWithSchema[VD: ClassTag, ED: ClassTag] extends TemporalGraph {
  def getSchema(): StructType

  def project[VD2: ClassTag, ED2: ClassTag](input: Seq[NamedExpression]): TemporalGraphWithSchema[VD2, ED2]
}
