package edu.drexel.cs.dbgroup.temporalgraph

import java.time.LocalDate
import java.time.temporal.ChronoUnit

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

  test("spilt function - 1monthresolution"){
    val resolution1Month = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2011-02-01"))
    val resolution1Year = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01"))
    val resolution1Day = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2011-01-02"))

    val error = intercept[IllegalArgumentException] {
      val spiltted = Interval(LocalDate.parse("2011-06-01"), LocalDate.parse("2013-04-01")).split(resolution1Month, LocalDate.parse("2011-06-02"))
    }
    info("markDate cannot be after interval start date passed")


    val spilttedByMonth = Interval(LocalDate.parse("2011-06-16"), LocalDate.parse("2012-04-10")).split(resolution1Month, LocalDate.parse("2011-06-01"))

    val spilttedByYear = Interval(LocalDate.parse("2011-06-16"), LocalDate.parse("2013-04-10")).split(resolution1Year, LocalDate.parse("2011-06-01"))

    val spilttedByDays = Interval(LocalDate.parse("2011-06-02"), LocalDate.parse("2011-06-11")).split(resolution1Day, LocalDate.parse("2011-06-01"))


    val expectedSplittedByMonth = Seq(
      (Interval(LocalDate.parse("2012-04-01"), LocalDate.parse("2012-04-10")),Interval(LocalDate.parse("2012-04-01"), LocalDate.parse("2012-05-01"))),
      (Interval(LocalDate.parse("2012-03-01"), LocalDate.parse("2012-04-01")),Interval(LocalDate.parse("2012-03-01"), LocalDate.parse("2012-04-01"))),
      (Interval(LocalDate.parse("2012-02-01"), LocalDate.parse("2012-03-01")),Interval(LocalDate.parse("2012-02-01"), LocalDate.parse("2012-03-01"))),
      (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2012-02-01")),Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2012-02-01"))),
      (Interval(LocalDate.parse("2011-12-01"), LocalDate.parse("2012-01-01")),Interval(LocalDate.parse("2011-12-01"), LocalDate.parse("2012-01-01"))),
      (Interval(LocalDate.parse("2011-11-01"), LocalDate.parse("2011-12-01")),Interval(LocalDate.parse("2011-11-01"), LocalDate.parse("2011-12-01"))),
      (Interval(LocalDate.parse("2011-10-01"), LocalDate.parse("2011-11-01")),Interval(LocalDate.parse("2011-10-01"), LocalDate.parse("2011-11-01"))),
      (Interval(LocalDate.parse("2011-09-01"), LocalDate.parse("2011-10-01")),Interval(LocalDate.parse("2011-09-01"), LocalDate.parse("2011-10-01"))),
      (Interval(LocalDate.parse("2011-08-01"), LocalDate.parse("2011-09-01")),Interval(LocalDate.parse("2011-08-01"), LocalDate.parse("2011-09-01"))),
      (Interval(LocalDate.parse("2011-07-01"), LocalDate.parse("2011-08-01")),Interval(LocalDate.parse("2011-07-01"), LocalDate.parse("2011-08-01"))),
      (Interval(LocalDate.parse("2011-06-16"), LocalDate.parse("2011-07-01")),Interval(LocalDate.parse("2011-06-01"), LocalDate.parse("2011-07-01")))
      )

    val expectedSplittedByYear = Seq(
      (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2013-04-10")),Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01"))),
      (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")),Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01"))),
      (Interval(LocalDate.parse("2011-06-16"), LocalDate.parse("2012-01-01")),Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")))
    )

    val expectedSplittedByDays = Seq(
      (Interval(LocalDate.parse("2011-06-10"), LocalDate.parse("2011-06-11")),Interval(LocalDate.parse("2011-06-10"), LocalDate.parse("2011-06-11"))),
      (Interval(LocalDate.parse("2011-06-09"), LocalDate.parse("2011-06-10")),Interval(LocalDate.parse("2011-06-09"), LocalDate.parse("2011-06-10"))),
      (Interval(LocalDate.parse("2011-06-08"), LocalDate.parse("2011-06-09")),Interval(LocalDate.parse("2011-06-08"), LocalDate.parse("2011-06-09"))),
      (Interval(LocalDate.parse("2011-06-07"), LocalDate.parse("2011-06-08")),Interval(LocalDate.parse("2011-06-07"), LocalDate.parse("2011-06-08"))),
      (Interval(LocalDate.parse("2011-06-06"), LocalDate.parse("2011-06-07")),Interval(LocalDate.parse("2011-06-06"), LocalDate.parse("2011-06-07"))),
      (Interval(LocalDate.parse("2011-06-05"), LocalDate.parse("2011-06-06")),Interval(LocalDate.parse("2011-06-05"), LocalDate.parse("2011-06-06"))),
      (Interval(LocalDate.parse("2011-06-04"), LocalDate.parse("2011-06-05")),Interval(LocalDate.parse("2011-06-04"), LocalDate.parse("2011-06-05"))),
      (Interval(LocalDate.parse("2011-06-03"), LocalDate.parse("2011-06-04")),Interval(LocalDate.parse("2011-06-03"), LocalDate.parse("2011-06-04"))),
      (Interval(LocalDate.parse("2011-06-02"), LocalDate.parse("2011-06-03")),Interval(LocalDate.parse("2011-06-02"), LocalDate.parse("2011-06-03")))
    )

    assert(expectedSplittedByMonth === spilttedByMonth)
    info("splitted by months passed")
    assert(expectedSplittedByYear === spilttedByYear)
    info("splitted by years passed")
    assert(expectedSplittedByDays === spilttedByDays)
    info("splitted by days passed")
  }

  test("Intersect with empty"){
    val one: Interval = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2014-02-01"))
    val empty: Interval = new Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2014-01-01"))
    assert(one.intersects(empty) == false)
    assert(empty.intersects(one) == false)
  }

}
