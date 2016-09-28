package edu.drexel.cs.dbgroup.temporalgraph

import edu.drexel.cs.dbgroup.temporalgraph.portal._
import edu.drexel.cs.dbgroup.temporalgraph.plans.PortalPlan

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.internal.CatalogImpl
import org.apache.spark.sql.catalyst.{CatalystConf,SimpleCatalystConf}
import org.apache.spark.sql.catalyst.catalog.{SessionCatalog,InMemoryCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry,Analyzer}
import org.apache.spark.sql.catalyst.optimizer.Optimizer

class PortalContext(@transient val sparkContext: SparkContext) {
  //TODO: move ProgramContext stuff in here

  //TODO: add conf stuff here
  lazy val catalog: Catalog = new CatalogImpl(ProgramContext.getSession)

  //TODO: create a new object with a valid registry
  lazy val functionRegistry: FunctionRegistry = PortalFunctionRegistry.builtin

  private lazy val conf: CatalystConf = new SimpleCatalystConf(false)
  lazy val sessionCatalog: SessionCatalog = new SessionCatalog(new InMemoryCatalog, functionRegistry, conf)

  protected[temporalgraph] lazy val analyzer: Analyzer = 
    new PortalAnalyzer(sessionCatalog, functionRegistry, conf)

  protected[temporalgraph] lazy val optimizer: Optimizer = new PortalOptimizer(sessionCatalog, conf)

  protected[temporalgraph] lazy val planner = new PortalPlanner()

  //the logical tree created by the parser does not contain all
  //the necessary elements. For example, it does not contain the analytics
  //nodes because that requires the analysis step
  def parsePortal(query: String): LogicalPlan = PortalParser.parse(query)
  def executePortal(query: String): this.QueryExecution = executePlan(parsePortal(query))
  def executePlan(plan: LogicalPlan) = new this.QueryExecution(plan)
  def portal(portalText: String): TGraphWProperties = executePortal(portalText).toTGraph

  protected[temporalgraph] class QueryExecution(val logical: LogicalPlan) {
    def assertAnalyzed(): Unit = analyzer.checkAnalysis(analyzed)

    lazy val analyzed: LogicalPlan = analyzer.execute(logical)
    lazy val optimizedPlan: LogicalPlan = optimizer.execute(analyzed)

    lazy val portalPlan: PortalPlan = {
      planner.plan(optimizedPlan).next()
    }


    lazy val toTGraph: TGraphWProperties = portalPlan.execute()
  }

}
