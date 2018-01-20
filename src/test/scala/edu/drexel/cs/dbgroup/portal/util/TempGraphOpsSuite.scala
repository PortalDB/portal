package edu.drexel.cs.dbgroup.portal.util

import java.sql.Date
import java.time.LocalDate
import org.apache.log4j.Level
import org.apache.log4j.Logger

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import edu.drexel.cs.dbgroup.portal.{Interval,ProgramContext}
import org.scalatest.{BeforeAndAfter, FunSuite}


class TempGraphOpsSuite extends FunSuite with BeforeAndAfter {
  before {
    if (ProgramContext.sc == null) {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      val conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME")).setMaster("local[2]")
      val sc = new SparkContext(conf)
      ProgramContext.setContext(sc)
      println(" ") //the first line starts from between
    }
  }

  test("intervalUnion and intervalIntersection"){

    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-01-01"))

    val intervals : RDD[Interval] = ProgramContext.sc.parallelize(Seq[Interval](testInterval1, testInterval2 , testInterval3))
    val intervalsOther : RDD[Interval] = ProgramContext.sc.parallelize(Seq[Interval](Interval(Date.valueOf("2011-01-01"), Date.valueOf("2015-01-01")),
      Interval(Date.valueOf("2015-01-01"), Date.valueOf("2017-01-01"))
    ))

    val resultUnion = TempGraphOps.intervalUnion(intervals, intervalsOther)
    val resultIntersection = TempGraphOps.intervalIntersect(intervals, intervalsOther)

    val expectedIntervalsIntersection : Seq[Interval] = Seq[Interval](Interval(Date.valueOf("2014-01-01"), Date.valueOf("2015-01-01")),
      Interval(Date.valueOf("2015-01-01"), Date.valueOf("2016-01-01")), Interval(Date.valueOf("2016-01-01"), Date.valueOf("2017-01-01"))
    )

    val expectedIntervalsUnion : Seq[Interval] = Seq[Interval](Interval(Date.valueOf("2011-01-01"), Date.valueOf("2014-01-01")),
      Interval(Date.valueOf("2014-01-01"), Date.valueOf("2015-01-01")),  Interval(Date.valueOf("2015-01-01"), Date.valueOf("2016-01-01")),
      testInterval2, testInterval3)

    assert(resultIntersection.collect.toList === expectedIntervalsIntersection)
    assert(resultUnion.collect.toList === expectedIntervalsUnion )
  }


  test("intervalUnion and intervalIntersection 2"){

    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-01-01"))

    val intervals : RDD[Interval] = ProgramContext.sc.parallelize(Seq[Interval](testInterval1, testInterval2 , testInterval3))
    val intervalsOther : RDD[Interval] = ProgramContext.sc.parallelize(Seq[Interval](Interval(Date.valueOf("2011-01-01"), Date.valueOf("2015-01-01")))
    )

    val resultUnion = TempGraphOps.intervalUnion(intervals, intervalsOther)
    val resultIntersection = TempGraphOps.intervalIntersect(intervals, intervalsOther)

    val expectedIntervalsIntersection : Seq[Interval] = Seq[Interval](Interval(Date.valueOf("2014-01-01"), Date.valueOf("2015-01-01")))

    val expectedIntervalsUnion : Seq[Interval] = Seq[Interval](Interval(Date.valueOf("2011-01-01"), Date.valueOf("2014-01-01")),
      Interval(Date.valueOf("2014-01-01"), Date.valueOf("2015-01-01")),  Interval(Date.valueOf("2015-01-01"), Date.valueOf("2016-01-01")),
      testInterval2, testInterval3)

    assert(resultIntersection.collect.toList === expectedIntervalsIntersection)
    assert(resultUnion.collect.toList === expectedIntervalsUnion )
  }

}
