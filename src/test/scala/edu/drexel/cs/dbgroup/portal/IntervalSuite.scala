package edu.drexel.cs.dbgroup.portal

import java.time.LocalDate
import java.time.temporal.ChronoUnit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfter, FunSuite}

class IntervalSuite extends FunSuite with BeforeAndAfter{

  before {
  }

  test("In constructor startDate cannot be after end date (IllegalArgumentException)"){
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

  test("toString method"){
    val start = LocalDate.parse("2012-01-01")
    val end = LocalDate.parse("2016-05-06")
    val testInterval = Interval(start, end)
    val actual = testInterval.toString()
    val expected = "[" + start.toString + "-" + end.toString + ")"
    assert(actual === expected)
  }

  test("equals method"){
    val start = LocalDate.parse("2012-01-01")
    val end = LocalDate.parse("2016-05-06")
    val testInterval1 = Interval(start, end)

    val start2 = LocalDate.parse("2012-01-01")
    val end2 = LocalDate.parse("2016-05-06")
    val testInterval2 = Interval(start2, end2)

    val start3 = LocalDate.parse("2013-01-01")
    val end3 = LocalDate.parse("2016-05-06")
    val testInterval3 = Interval(start3, end3)

    assert(testInterval1.equals(testInterval2))
    assert(testInterval1 != testInterval3)
    assert(testInterval2 != testInterval3)
  }

  test("contains(Interval) method"){
    val testInterval1 = Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01"))
    val testInterval4 = Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2013-12-01"))

    //completely inside -true
    assert(testInterval1.contains(testInterval4))
    //inside with same end date -true
    assert(testInterval1.contains(testInterval2))
    //inside with same start date -true
    assert(testInterval1.contains(testInterval3))

    //partially inside -false
    assert(!testInterval2.contains(testInterval3))
    //partially inside -false
    assert(!testInterval3.contains(testInterval2))
    //completely outside -false
    assert(!testInterval2.contains(testInterval4))
  }

  test("contains(Date) method"){
    val testInterval = Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2016-01-01"))
    val date1 = LocalDate.parse("2013-01-01")
    val date2 = LocalDate.parse("2012-01-01")
    val date3 = LocalDate.parse("2016-01-01")
    val date4 = LocalDate.parse("1999-01-01")

    //is inside -true
    assert(testInterval.contains(date1))
    //is start -true
    assert(testInterval.contains(date2))
    // is end -false
    assert(!testInterval.contains(date3))
    //is outside -false
    assert(!testInterval.contains(date4))
  }

  test("intersects(Interval) method"){
    val testInterval1 = Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01"))
    val testInterval4 = Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2013-12-01"))
    val testInterval5 = Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01"))


    //completely inside - true
    assert(testInterval1.intersects(testInterval4))
    //inside with same end date - true
    assert(testInterval1.intersects(testInterval2))
    //inside with same start date - true
    assert(testInterval1.intersects(testInterval3))
    //partially inside - true
    assert(testInterval2.intersects(testInterval3))
    //partially inside - true
    assert(testInterval3.intersects(testInterval2))
    //completely outside - false
    assert(!testInterval2.intersects(testInterval4))
    //completely outside where end date equals start date - false
    assert(!testInterval2.intersects(testInterval5))
  }

  test("isempty"){
    val testInterval1 = Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2012-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2012-01-02"))

    //empty -true
    assert(testInterval1.isEmpty())
    //empty -false
    assert(!testInterval2.isEmpty())
  }

}
