package edu.drexel.cs.dbgroup.temporalgraph.plan

import edu.drexel.cs.dbgroup.temporalgraph.{TemporalGraphWithSchema,VertexEdgeAttribute}
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.spark.sql.catalyst.plans.QueryPlan

abstract class PortalPlan extends QueryPlan[PortalPlan] {

  /**
   * Whether the "prepare" method is called.
   */
  private val prepareCalled = new AtomicBoolean(false)

  /**
   * Returns the result of this query as an TemporalGraphWithSchema by delegating to doExecute
   * Concrete implementations of PortalPlan should override doExecute instead.
   */
  final def execute(): TemporalGraphWithSchema = {
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
   * Produces the result of the query as an TemporalGraphWithSchema
   */
  protected def doExecute(): TemporalGraphWithSchema

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

