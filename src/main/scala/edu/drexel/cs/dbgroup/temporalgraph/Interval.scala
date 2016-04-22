package edu.drexel.cs.dbgroup.temporalgraph

import scala.math.Ordered.orderingToOrdered
import scala.math.Ordering._
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps._

/**
  * Time period with a closed-open model, i.e. [start, end)
  * A time period where start=end is null/empty.
  */
class Interval(st: LocalDate, en: LocalDate) extends Ordered[Interval] with Serializable {
  val start:LocalDate = st
  val end:LocalDate = en

  override def toString():String = {
    "[" + start.toString + "-" + end.toString + ")"
  }

  override def equals(other:Any):Boolean = {
    other match {
      case in: Interval => start == in.start && end == in.end
      case _ => false
    }
    
  }

  override def hashCode: Int = toString.hashCode()

  def contains(other: Interval):Boolean = {
    if ((other.start.isAfter(start) || other.start.isEqual(start)) && (other.end.isBefore(end) || other.end.isEqual(end)))
      true
    else
      false
  }

  def contains(num: LocalDate):Boolean = {
    if ((num.isAfter(start) || num.isEqual(start)) && num.isBefore(end))
      true
    else
      false
  }

  def compare(other: Interval): Int = {
    if (start.isEqual(other.start) && end.isEqual(other.end))
      0
    else if (start.isBefore(other.start))
      -1
    else
      1
  }

  //if the other interval has any (including complete) overlap in years, return true
  def intersects(other: Interval):Boolean = {
    if (other.start.isAfter(end) || other.start.equals(end) || other.end.isBefore(start) || other.end.equals(start))
      false
    else
      true
  }

  /*
   * Calculate a period of intersection.
   */
  def intersection(other: Interval): Option[Interval] = {
    val rest: LocalDate = maxDate(start, other.start)
    val ree: LocalDate = minDate(end, other.end)
    if (rest.isAfter(ree) || rest.equals(ree))
      None
    else
      Some(Interval(rest, ree))
  }

  def isEmpty():Boolean = start == end

  /*
   * Calculate how much of this interval covers the other interval
   * as a ratio from 0 to 1 where 1 means they are equal,
   * and 0 means they do not intersect.
   * This interval should be a subset of other
   * to get meaningful results.
   */
  def ratio(other: Interval): Double = {
    if (this == other)
      1.0
    else
      ChronoUnit.DAYS.between(this.start, this.end) / ChronoUnit.DAYS.between(other.start, other.end).toDouble
  }

  /*
   * Splits this period into as many parts as time windows it covers.
   * The results are in reverse order, from latest to earliest.
   * For each period, the coverage is computed as a ratio (0-1)
   */
  def split(period: Resolution, mark: LocalDate): Seq[(Interval, Double, Interval)] = {
    if(mark.isAfter(this.start))
      throw new IllegalArgumentException("markDate cannot be after interval start date")
    var res: List[(Interval,Double,Interval)] = List()
    var markStart: LocalDate = start
    var markEnd: LocalDate = mark

    while (end.isAfter(markEnd)) {
      val step = period.getInterval(markEnd)
      if (markStart.isBefore(step.end)) {
        val nextIntv: Interval = if (markStart == step.start && end == step.end) step else Interval(markStart, minDate(step.end, end))
        val ratio: Double = nextIntv.ratio(step)
        res = (nextIntv, ratio, step) :: res
        markStart = step.end
      }
      markEnd = step.end
    }

    res
  }

}

object Interval {
  def apply(mn: LocalDate, mx: LocalDate) =
   if(mn.isAfter(mx))
     throw new IllegalArgumentException("StartDate cannot be after end date")
   else
      new Interval(mn,mx)
}
