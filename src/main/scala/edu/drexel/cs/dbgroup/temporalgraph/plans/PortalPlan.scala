package edu.drexel.cs.dbgroup.temporalgraph.plans

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StructType,Metadata,StructField}

import edu.drexel.cs.dbgroup.temporalgraph.{TGraphWProperties,VertexEdgeAttribute}

abstract class PortalPlan extends QueryPlan[PortalPlan] {

  /**
   * Whether the "prepare" method is called.
   */
  private val prepareCalled = new AtomicBoolean(false)

  /**
   * Returns the result of this query as an TemporalGraphWithSchema by delegating to doExecute
   * Concrete implementations of PortalPlan should override doExecute instead.
   */
  final def execute(): TGraphWProperties = {
    prepare()
    doExecute()
  }
  
  /**
   * Prepare a PortalPlan for execution.
   */
  final def prepare(): Unit = {
    if (prepareCalled.compareAndSet(false, true)) {
      doPrepare()
      children.foreach(_.prepare())
    }
  }

  /**
   * Overridden by concrete implementations of PortalPlan. It is guaranteed to run before any
   * `execute` of PortalPlan. This is helpful if we want to set up some state before executing the
   * query
   *
   * Note: the prepare method has already walked down the tree, so the implementation doesn't need
   * to call children's prepare methods.
   */
  protected def doPrepare(): Unit = {}

  /**
   * Overridden by concrete implementations of PortalPlan.
   * Produces the result of the query as an TGraphWProperties
   */
  protected def doExecute(): TGraphWProperties

  //since we are schema-less, we use this kind of hack
  lazy val catalog: Seq[Attribute] = Seq(AttributeReference("V", StructType(Seq[StructField]()), false, Metadata.empty)(), AttributeReference("E", StructType(Seq[StructField]()), false, Metadata.empty)())

}

trait LeafNode extends PortalPlan {
  override def children: Seq[PortalPlan] = Nil
}

trait UnaryNode extends PortalPlan {
  def child: PortalPlan

  override def children: Seq[PortalPlan] = child :: Nil

}

trait BinaryNode extends PortalPlan {
  def left: PortalPlan
  def right: PortalPlan

  override def children: Seq[PortalPlan] = Seq(left, right)
}

