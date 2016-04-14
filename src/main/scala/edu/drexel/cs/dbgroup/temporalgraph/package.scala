package edu.drexel.cs.dbgroup

import org.apache.spark.SparkContext

package object temporalgraph {
  /**
    * A time interval identifier, 0-indexed.
    */
  type TimeIndex = Int

  trait Quantification extends Serializable
  case class Always extends Quantification
  case class Exists extends Quantification
  case class Most extends Quantification
  case class AtLeast(ratio: Double) extends Quantification


  object ProgramContext {
    var sc:SparkContext = null

    def setContext(c: SparkContext):Unit = sc = c
  }

  object PartitionStrategyType extends Enumeration {
    val CanonicalRandomVertexCut, EdgePartition2D, NaiveTemporal, NaiveTemporalEdge, ConsecutiveTemporal, ConsecutiveTemporalEdge, HybridRandomTemporal, HybridRandomEdgeTemporal, Hybrid2DTemporal, Hybrid2DEdgeTemporal, None = Value
  }

  trait WindowSpecification extends Serializable

}
