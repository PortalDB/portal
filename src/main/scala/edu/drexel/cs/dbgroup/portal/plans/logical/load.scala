package edu.drexel.cs.dbgroup.portal.plans.logical

import java.time.LocalDate

import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StructType,Metadata,StructField}

import edu.drexel.cs.dbgroup.portal.GraphSpec
import edu.drexel.cs.dbgroup.portal.util.GraphLoader
import edu.drexel.cs.dbgroup.portal.portal.PortalException

case class LoadGraph(url: String) extends LeafNode {
  override def output: Seq[Attribute] = catalog

  private val fullSpec: GraphSpec = GraphLoader.loadGraphDescription(url)

  //since we are schema-less, this gives us property names and types
  //but each vertex/edge does not have to have all or any of them
  lazy val catalog: Seq[Attribute] = fullSpec.toAttributes()

}

