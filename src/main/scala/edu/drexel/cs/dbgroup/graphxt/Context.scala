package edu.drexel.cs.dbgroup.graphxt

import org.apache.spark.SparkContext

object ProgramContext {
  var sc:SparkContext = null

  def setContext(c: SparkContext):Unit = sc = c
}
