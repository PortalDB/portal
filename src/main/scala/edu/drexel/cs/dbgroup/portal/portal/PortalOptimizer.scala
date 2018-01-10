package edu.drexel.cs.dbgroup.portal.portal

import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.optimizer.Optimizer

class PortalOptimizer(catalog: SessionCatalog, conf: CatalystConf) extends Optimizer(catalog, conf) {
  //ALL OPTIMIZING IS TODO for the time being

  //TODO: rule for pushing presence of aggregations and
  //snapshot analytics into load leaves for DS selection
  //TODO: push selection into load
  override val batches = Seq[Batch]()
}
