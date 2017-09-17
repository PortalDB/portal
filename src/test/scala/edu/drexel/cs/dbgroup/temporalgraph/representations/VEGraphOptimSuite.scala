package edu.drexel.cs.dbgroup.temporalgraph.representations

import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph.{Always, ChangeSpec, Exists, Resolution, TimeSpec, _}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Created by amir on 12/15/16.
  */
class VEGraphOptimSuite  extends VEGraphSuite {
  override lazy val empty = VEGraphOptim.emptyGraph[String,Int]("Default")

}
