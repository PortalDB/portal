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

}
