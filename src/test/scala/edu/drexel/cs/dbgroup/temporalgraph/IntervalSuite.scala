package edu.drexel.cs.dbgroup.temporalgraph

import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfter, FunSuite}

class IntervalSuite extends FunSuite with BeforeAndAfter{

  before {
    if(ProgramContext.sc == null){
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME")).setMaster("local[2]")
      val sc = new SparkContext(conf)
      ProgramContext.setContext(sc)
    }
  }

  test("StartDate should not be after end date (IllegalArgumentException)"){
    val error = intercept[IllegalArgumentException] {
      Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2013-01-01"))
    }
  }

}
