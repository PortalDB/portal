package edu.drexel.cs.dbgroup.graphxt

import java.time.Period
import java.time.LocalDate
import java.time.temporal.TemporalUnit
import java.time.temporal.ChronoUnit

/**
  * Resolution of a time period, in semantic units,
  * i.e. days, months, weeks, years
  */
class Resolution(per:Period) {

  val period:Period = per

  lazy val unit:ChronoUnit = {
    if (period.getDays() > 0)
      ChronoUnit.DAYS
    else if (period.getMonths() > 0)
      ChronoUnit.MONTHS
    else
      ChronoUnit.YEARS
  }

  def isValid():Boolean = {
    //can only have 1 unit set, i.e. only days or years
    var sum:Int = 0
    if (period.getDays() > 0) sum += 1
    if (period.getMonths() > 0) sum += 1
    if (period.getYears() > 0) sum += 1

    if (sum > 1)
      false
    else
      true
  }

  def get(u: TemporalUnit):Long = period.get(u)

  def greater(other: Resolution):Boolean = {
    if (unit == other.unit && get(unit) > other.get(unit))
      true
    else if (unit.compareTo(other.unit) > 0)
      true
    else
      false
  }

  /**
    * Test to see if it is valid to go from this resolution
    * to a new target resolution
    */
  def isCompatible(target: Resolution):Boolean = {
    if (greater(target))
      return false

    if (!isValid() || !target.isValid()) {
      throw new IllegalArgumentException("invalid resolution")
    }

    //resolutions are compatible if it is possible to go from smaller to larger
    unit match {
      case ChronoUnit.DAYS =>
        target.unit match {
          case ChronoUnit.DAYS =>
            if (target.period.getDays() % period.getDays() == 0)
              true
            else
              false
          case ChronoUnit.MONTHS => 
            if (period.getDays() == 1)
              true
            else
              false
          case ChronoUnit.YEARS => 
            if (period.getDays() == 1)
              true
            else
              false
          case _ =>  //should never get here
            false
        }
      case ChronoUnit.MONTHS =>
        target.unit match {
          case ChronoUnit.MONTHS =>
            if (target.period.getMonths() % period.getMonths() == 0)
              true
            else
              false
          case ChronoUnit.YEARS =>
            if (period.getMonths() == 1)
              true
            else
              false
          case _ =>
              false
        }
      case ChronoUnit.YEARS =>
        if (target.unit == ChronoUnit.YEARS && target.period.getYears() % period.getYears() == 0)
          true
        else
          false
      case _ =>  //should never get here
        false
    }
  }

  /**
    * Create an interval based on this resolution around a specific date as anchor.
    * For example, if resolution if 1 month, then any date within a month
    * returns a [mo/01/yr, nextmo/01/yr] interval.
    * Resolutions are semantic
    */
  def getInterval(date:LocalDate):Interval = {
    unit match {
      case ChronoUnit.DAYS =>
        Interval(date, date.plusDays(period.getDays()))
      case ChronoUnit.MONTHS =>
        Interval(date.withDayOfMonth(1), date.withDayOfMonth(1).plusMonths(period.getMonths()))
      case ChronoUnit.YEARS =>
        Interval(date.withDayOfYear(1), date.withDayOfYear(1).plusYears(period.getYears()))
      case _ =>  //should never get here
        Interval(date, date)
    }
  }

  /**
    * Compute the number of intervals that fit within this new resolution.
    * I.e., if going from intervals in days to months, dates in april will return 30
    */
  def getNumParts(other:Resolution, anchor:LocalDate):Int = {
    if (!isValid() || !other.isValid()) {
      throw new IllegalArgumentException("invalid resolution")
    }

    //if the other resolution is larger, then we can't fit any intervals
    //because it is not valid to go from a larger resolution to smaller
    if (greater(other))
      return 0

    other.unit match {
      case ChronoUnit.DAYS =>
        unit match {
          case ChronoUnit.DAYS =>
            period.getDays() / other.period.getDays()
          case ChronoUnit.MONTHS =>
            //how many days are in the month of the anchor?
            anchor.lengthOfMonth()
          case ChronoUnit.YEARS =>
            //how many days in the year of the anchor?
            anchor.lengthOfYear()
          case _ =>  //should never get here
            0
        }
      case ChronoUnit.MONTHS =>
        unit match {
          case ChronoUnit.MONTHS =>
            period.getMonths() / other.period.getMonths()
          case ChronoUnit.YEARS =>
            12
          case _ => //shouldn't ever get here
            0
        }
      case ChronoUnit.YEARS =>
        unit match {
          case ChronoUnit.YEARS =>
            period.getYears() / other.period.getYears()
          case _ => //shouldn't ever get here
            0
        }
      case _ =>  //should never get here
        0
    }
  }

  /**
    * Compute how many intervals fit between two dates with this resolution
    */
  def numBetween(start:LocalDate, end:LocalDate):Long = {
    start.until(end, unit)
  }
}

object Resolution {
  def between(st:LocalDate, en:LocalDate):Resolution = new Resolution(Period.between(st, en))
  def from(st:String):Resolution = new Resolution(Period.parse(st))
}
