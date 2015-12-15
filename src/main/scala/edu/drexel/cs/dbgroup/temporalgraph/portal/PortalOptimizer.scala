package edu.drexel.cs.dbgroup.temporalgraph.portal

import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.optimizer.Optimizer

object PortalOptimizer extends Optimizer {
  //ALL OPTIMIZING IS TODO for the time being

  //TODO: rule for pushing presence of aggregations and
  //snapshot analytics into load leaves for DS selection
  //TODO: push selection into load
  val batches = Seq[Batch]()
}
