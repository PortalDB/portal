package edu.drexel.cs.dbgroup.temporalgraph.util

import java.sql.Date
import java.time.LocalDate

import edu.drexel.cs.dbgroup.temporalgraph.Interval
import org.scalatest.{BeforeAndAfter, FunSuite}


class TempGraphOpsSuite extends FunSuite with BeforeAndAfter {

  test("intervalUnion and intervalIntersection"){

    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-01-01"))

    val intervals : Seq[Interval] = Seq[Interval](testInterval1, testInterval2 , testInterval3)
    val intervalsOther : Seq[Interval] = Seq[Interval](Interval(Date.valueOf("2011-01-01"), Date.valueOf("2015-01-01")),
      Interval(Date.valueOf("2015-01-01"), Date.valueOf("2017-01-01"))
    )

    val resultUnion = TempGraphOps.intervalUnion(intervals, intervalsOther)
    val resultIntersection = TempGraphOps.intervalIntersect(intervals, intervalsOther)

    val expectedIntervalsIntersection : Seq[Interval] = Seq[Interval](Interval(Date.valueOf("2014-01-01"), Date.valueOf("2015-01-01")),
      Interval(Date.valueOf("2015-01-01"), Date.valueOf("2016-01-01")), Interval(Date.valueOf("2016-01-01"), Date.valueOf("2017-01-01"))
    )

    val expectedIntervalsUnion : Seq[Interval] = Seq[Interval](Interval(Date.valueOf("2011-01-01"), Date.valueOf("2014-01-01")),
      Interval(Date.valueOf("2014-01-01"), Date.valueOf("2015-01-01")),  Interval(Date.valueOf("2015-01-01"), Date.valueOf("2016-01-01")),
      testInterval2, testInterval3)

    assert(resultIntersection === expectedIntervalsIntersection)
    assert(resultUnion === expectedIntervalsUnion )
  }


  test("intervalUnion and intervalIntersection 2"){

    val testInterval1 = Interval(LocalDate.parse("2014-01-01"), LocalDate.parse("2016-01-01"))
    val testInterval2 = Interval(LocalDate.parse("2016-01-01"), LocalDate.parse("2017-01-01"))
    val testInterval3 = Interval(LocalDate.parse("2017-01-01"), LocalDate.parse("2018-01-01"))

    val intervals : Seq[Interval] = Seq[Interval](testInterval1, testInterval2 , testInterval3)
    val intervalsOther : Seq[Interval] = Seq[Interval](Interval(Date.valueOf("2011-01-01"), Date.valueOf("2015-01-01"))
    )

    val resultUnion = TempGraphOps.intervalUnion(intervals, intervalsOther)
    val resultIntersection = TempGraphOps.intervalIntersect(intervals, intervalsOther)

    val expectedIntervalsIntersection : Seq[Interval] = Seq[Interval](Interval(Date.valueOf("2014-01-01"), Date.valueOf("2015-01-01")))

    val expectedIntervalsUnion : Seq[Interval] = Seq[Interval](Interval(Date.valueOf("2011-01-01"), Date.valueOf("2014-01-01")),
      Interval(Date.valueOf("2014-01-01"), Date.valueOf("2015-01-01")),  Interval(Date.valueOf("2015-01-01"), Date.valueOf("2016-01-01")),
      testInterval2, testInterval3)

    assert(resultIntersection === expectedIntervalsIntersection)
    assert(resultUnion === expectedIntervalsUnion )
  }

}
