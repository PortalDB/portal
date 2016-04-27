package edu.drexel.cs.dbgroup

import org.apache.spark.SparkContext

package object temporalgraph {
  /**
    * A time interval identifier, 0-indexed.
    */
  type TimeIndex = Int

  trait Quantification extends Serializable {
    def keep(in: Double): Boolean
  }
  case class Always extends Quantification {
    override def keep(in: Double): Boolean = in > 0.99
  }
  case class Exists extends Quantification {
    override def keep(in: Double): Boolean = true
  }
  case class Most extends Quantification {
    override def keep(in: Double): Boolean = in > 0.5
  }
  case class AtLeast(ratio: Double) extends Quantification {
    override def keep(in: Double): Boolean = in >= ratio
  }

  object ProgramContext {
    var sc:SparkContext = null

    def setContext(c: SparkContext):Unit = sc = c
  }

  object PartitionStrategyType extends Enumeration {
    val CanonicalRandomVertexCut, EdgePartition2D, NaiveTemporal, NaiveTemporalEdge, ConsecutiveTemporal, ConsecutiveTemporalEdge, HybridRandomTemporal, HybridRandomEdgeTemporal, Hybrid2DTemporal, Hybrid2DEdgeTemporal, None = Value
  }

  trait WindowSpecification extends Serializable
  case class ChangeSpec(num: Integer) extends WindowSpecification
  case class TimeSpec(res: Resolution) extends WindowSpecification

}
