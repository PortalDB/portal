package edu.drexel.cs.dbgroup.temporalgraph

import java.time.LocalDate
import java.time.temporal.ChronoUnit

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

  test("spilt function - 1monthresolution"){
    val resolution1Month = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2011-02-01"))
    val resolution1Year = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01"))
    val resolution1Day = Resolution.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2011-01-02"))

    val error = intercept[IllegalArgumentException] {
      val spiltted = Interval(LocalDate.parse("2011-06-01"), LocalDate.parse("2013-04-01")).split(resolution1Month, LocalDate.parse("2011-06-02"))
    }
    info("markDate cannot be after interval start date passed")


    val spilttedByMonth = Interval(LocalDate.parse("2011-06-16"), LocalDate.parse("2012-04-10")).split(resolution1Month, LocalDate.parse("2011-06-01"))
    println("by month")
    spilttedByMonth.foreach(println)

    val spilttedByYear = Interval(LocalDate.parse("2011-06-16"), LocalDate.parse("2013-04-10")).split(resolution1Year, LocalDate.parse("2011-06-01"))
    println("by Year")
    spilttedByYear.foreach(println)

    println("by days")
    val spilttedByDays = Interval(LocalDate.parse("2011-06-02"), LocalDate.parse("2011-06-11")).split(resolution1Day, LocalDate.parse("2011-06-01"))
    spilttedByDays.foreach(println)


    val expectedSplittedByMonth = Seq(
      (Interval(LocalDate.parse("2012-04-01"), LocalDate.parse("2012-04-10")),0.3,Interval(LocalDate.parse("2012-04-01"), LocalDate.parse("2012-05-01"))),
      (Interval(LocalDate.parse("2012-03-01"), LocalDate.parse("2012-04-01")),1.0,Interval(LocalDate.parse("2012-03-01"), LocalDate.parse("2012-04-01"))),
      (Interval(LocalDate.parse("2012-02-01"), LocalDate.parse("2012-03-01")),1.0,Interval(LocalDate.parse("2012-02-01"), LocalDate.parse("2012-03-01"))),
      (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2012-02-01")),1.0,Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2012-02-01"))),
      (Interval(LocalDate.parse("2011-12-01"), LocalDate.parse("2012-01-01")),1.0,Interval(LocalDate.parse("2011-12-01"), LocalDate.parse("2012-01-01"))),
      (Interval(LocalDate.parse("2011-11-01"), LocalDate.parse("2011-12-01")),1.0,Interval(LocalDate.parse("2011-11-01"), LocalDate.parse("2011-12-01"))),
      (Interval(LocalDate.parse("2011-10-01"), LocalDate.parse("2011-11-01")),1.0,Interval(LocalDate.parse("2011-10-01"), LocalDate.parse("2011-11-01"))),
      (Interval(LocalDate.parse("2011-09-01"), LocalDate.parse("2011-10-01")),1.0,Interval(LocalDate.parse("2011-09-01"), LocalDate.parse("2011-10-01"))),
      (Interval(LocalDate.parse("2011-08-01"), LocalDate.parse("2011-09-01")),1.0,Interval(LocalDate.parse("2011-08-01"), LocalDate.parse("2011-09-01"))),
      (Interval(LocalDate.parse("2011-07-01"), LocalDate.parse("2011-08-01")),1.0,Interval(LocalDate.parse("2011-07-01"), LocalDate.parse("2011-08-01"))),
      (Interval(LocalDate.parse("2011-06-16"), LocalDate.parse("2011-07-01")),0.5,Interval(LocalDate.parse("2011-06-01"), LocalDate.parse("2011-07-01")))
      )

    val ratio1: Double = ChronoUnit.DAYS.between(LocalDate.parse("2013-01-01"), LocalDate.parse("2013-04-10")) / ChronoUnit.DAYS.between(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01")).toDouble
    val ratio2: Double = ChronoUnit.DAYS.between(LocalDate.parse("2011-06-16"), LocalDate.parse("2012-01-01")) / ChronoUnit.DAYS.between(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")).toDouble
    println(ratio1)
    println(ratio2)
    val expectedSplittedByYear = Seq(
      (Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2013-04-10")),ratio1,Interval(LocalDate.parse("2013-01-01"), LocalDate.parse("2014-01-01"))),
      (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01")),1.0,Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2013-01-01"))),
      (Interval(LocalDate.parse("2011-06-16"), LocalDate.parse("2012-01-01")),ratio2,Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")))
    )

    val expectedSplittedByDays = Seq(
      (Interval(LocalDate.parse("2011-06-10"), LocalDate.parse("2011-06-11")),1.0,Interval(LocalDate.parse("2011-06-10"), LocalDate.parse("2011-06-11"))),
      (Interval(LocalDate.parse("2011-06-09"), LocalDate.parse("2011-06-10")),1.0,Interval(LocalDate.parse("2011-06-09"), LocalDate.parse("2011-06-10"))),
      (Interval(LocalDate.parse("2011-06-08"), LocalDate.parse("2011-06-09")),1.0,Interval(LocalDate.parse("2011-06-08"), LocalDate.parse("2011-06-09"))),
      (Interval(LocalDate.parse("2011-06-07"), LocalDate.parse("2011-06-08")),1.0,Interval(LocalDate.parse("2011-06-07"), LocalDate.parse("2011-06-08"))),
      (Interval(LocalDate.parse("2011-06-06"), LocalDate.parse("2011-06-07")),1.0,Interval(LocalDate.parse("2011-06-06"), LocalDate.parse("2011-06-07"))),
      (Interval(LocalDate.parse("2011-06-05"), LocalDate.parse("2011-06-06")),1.0,Interval(LocalDate.parse("2011-06-05"), LocalDate.parse("2011-06-06"))),
      (Interval(LocalDate.parse("2011-06-04"), LocalDate.parse("2011-06-05")),1.0,Interval(LocalDate.parse("2011-06-04"), LocalDate.parse("2011-06-05"))),
      (Interval(LocalDate.parse("2011-06-03"), LocalDate.parse("2011-06-04")),1.0,Interval(LocalDate.parse("2011-06-03"), LocalDate.parse("2011-06-04"))),
      (Interval(LocalDate.parse("2011-06-02"), LocalDate.parse("2011-06-03")),1.0,Interval(LocalDate.parse("2011-06-02"), LocalDate.parse("2011-06-03")))
    )

    assert(expectedSplittedByMonth === spilttedByMonth)
    info("splitted by months passed")
    assert(expectedSplittedByYear === spilttedByYear)
    info("splitted by years passed")
    assert(expectedSplittedByDays === spilttedByDays)
    info("splitted by days passed")
  }


}
