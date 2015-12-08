package edu.drexel.cs.dbgroup

import org.apache.spark.SparkContext

package object temporalgraph {
  /**
    * A time interval identifier, 0-indexed.
    */
  type TimeIndex = Int

  object AggregateSemantics extends Enumeration {
    val Existential, Universal = Value
  }

  object ProgramContext {
    var sc:SparkContext = null

    def setContext(c: SparkContext):Unit = sc = c
  }

  object PartitionStrategyType extends Enumeration {
    val CanonicalRandomVertexCut, EdgePartition2D, NaiveTemporal, NaiveTemporalEdge, ConsecutiveTemporal, ConsecutiveTemporalEdge, HybridRandomTemporal, HybridRandomEdgeTemporal, Hybrid2DTemporal, Hybrid2DEdgeTemporal, None = Value
  }

}
