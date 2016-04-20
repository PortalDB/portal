package edu.drexel.cs.dbgroup.temporalgraph

import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfter, FunSuite}

class IntervalSuite extends FunSuite with BeforeAndAfter{

  before {
  }

  test("StartDate should not be after end date (IllegalArgumentException)"){
    val error = intercept[IllegalArgumentException] {
      Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2013-01-01"))
    }
  }

  test("Intersect with empty"){
    val one: Interval = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2014-02-01"))
    val empty: Interval = new Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2014-01-01"))
    assert(one.intersects(empty) == false)
    assert(empty.intersects(one) == false)
  }

}
