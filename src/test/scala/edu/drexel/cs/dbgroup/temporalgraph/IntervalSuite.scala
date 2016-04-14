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

  test("In constructor startDate cannot be after end date (IllegalArgumentException)"){
    val error = intercept[IllegalArgumentException] {
      Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2013-01-01"))
    }
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
