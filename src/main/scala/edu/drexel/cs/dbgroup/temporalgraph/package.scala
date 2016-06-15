package edu.drexel.cs.dbgroup

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

package object temporalgraph {
  /**
    * A time interval identifier, 0-indexed.
    */
  type TimeIndex = Int

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
    @transient private var sqlc:SQLContext = _

    def setContext(c: SparkContext):Unit = sc = c
    def getSqlContext:SQLContext = {
      //hive context is usually recommended to be used instead of plain sqlcontext
      if (sqlc == null) sqlc = new org.apache.spark.sql.SQLContext(sc)
      sqlc
    }
    lazy val eagerCoalesce: Boolean = if (sc.getConf.get("portal.coalesce", "lazy") == "eager") true else false
  }

  object PartitionStrategyType extends Enumeration {
    val CanonicalRandomVertexCut, EdgePartition2D, NaiveTemporal, NaiveTemporalEdge, ConsecutiveTemporal, ConsecutiveTemporalEdge, HybridRandomTemporal, HybridRandomEdgeTemporal, Hybrid2DTemporal, Hybrid2DEdgeTemporal, None = Value
  }

  trait WindowSpecification extends Serializable
  case class ChangeSpec(num: Integer) extends WindowSpecification
  case class TimeSpec(res: Resolution) extends WindowSpecification

}
