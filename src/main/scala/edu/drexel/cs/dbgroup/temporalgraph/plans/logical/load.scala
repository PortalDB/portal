package edu.drexel.cs.dbgroup.temporalgraph.plans.logical

import java.time.LocalDate

import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StructType,Metadata,StructField}

import edu.drexel.cs.dbgroup.temporalgraph.util.GraphLoader
import edu.drexel.cs.dbgroup.temporalgraph.portal.PortalException

case class LoadGraph(url: String) extends LeafNode {
  override def output: Seq[Attribute] = catalog
  //since we are schema-less, we use this kind of hack
  lazy val catalog: Seq[Attribute] = Seq(AttributeReference("V", StructType(Seq[StructField]()), false, Metadata.empty)(), AttributeReference("E", StructType(Seq[StructField]()), false, Metadata.empty)())

}

