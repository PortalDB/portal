package edu.drexel.cs.dbgroup

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

package object temporalgraph {

  /**
    * Unique Identifier for an Edge
    */
  type EdgeId = Long
  /**
    * A time interval identifier, 0-indexed.
    */
  type TimeIndex = Int
  /**
    * Attribute of graphs when there are no attributes.
    * Picked boolean because it is very small in memory
    */
  type StructureOnlyAttr = Boolean

  trait Quantification extends Serializable {
    def keep(in: Double): Boolean
    def threshold: Double
  }
  case class Always() extends Quantification {
    override def keep(in: Double): Boolean = in > 0.99
    override def threshold: Double = 0.99
  }
  case class Exists() extends Quantification {
    override def keep(in: Double): Boolean = in > 0.0
    override def threshold: Double = 0.0
  }
  case class Most() extends Quantification {
    override def keep(in: Double): Boolean = in > 0.5
    override def threshold: Double = 0.5
  }
  case class AtLeast(ratio: Double) extends Quantification {
    override def keep(in: Double): Boolean = in >= ratio
    override def threshold: Double = ratio
  }

  object ProgramContext {
    @transient var sc:SparkContext = _
    @transient private var session: SparkSession = _

    def setContext(c: SparkContext):Unit = sc = c
    def getSession: SparkSession = {
      if (session == null) session = SparkSession.builder().getOrCreate()
      session
    }
    lazy val eagerCoalesce: Boolean = if (sc.getConf.get("portal.coalesce", "lazy") == "eager") true else false
  }

  object PartitionStrategyType extends Enumeration {
    val CanonicalRandomVertexCut, EdgePartition2D, NaiveTemporal, NaiveTemporalEdge, ConsecutiveTemporal, ConsecutiveTemporalEdge, HybridRandomTemporal, HybridRandomEdgeTemporal, Hybrid2DTemporal, Hybrid2DEdgeTemporal, None = Value
  }

  case class TGraphPartitioning(pst: PartitionStrategyType.Value = PartitionStrategyType.None, runs: Int = 1, parts: Int = 0)

  trait WindowSpecification extends Serializable
  case class ChangeSpec(num: Integer) extends WindowSpecification
  case class TimeSpec(res: Resolution) extends WindowSpecification

  object Locality extends Enumeration {
    val Temporal, Structural = Value
  }
  object SnapshotGroup extends Enumeration {
    val None, WidthTime, WidthRGs, Depth, Redundancy = Value
  }

}
