package edu.drexel.cs.dbgroup.temporalgraph

import scala.math.Ordered.orderingToOrdered
import scala.math.Ordering._
import java.time.LocalDate
import java.time.Period

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
    if (resolution.equals(other.resolution)) {
      if (start.isEqual(other.start) && end.isEqual(other.end))
        0
      else if (start.isBefore(other.start))
        -1
      else
        1
    } else {
      throw new IllegalArgumentException("intervals of different resolutions are not comparable")
    }
  }

  //if the other interval has any (including complete) overlap in years, return true
  def intersects(other: Interval):Boolean = {
    if (other.start.isAfter(end) || other.start.equals(end) || other.end.isBefore(start) || other.end.equals(start))
      false
    else
      true
  }

  def isEmpty():Boolean = start == end

  def isUnionCompatible(other:Interval):Boolean = {
    if (resolution.isEqual(other.resolution)) {
      var st:LocalDate = start
      var en:LocalDate = end
      if (start.isBefore(other.start)) {
        st = start
        en = other.start
      } else {
        st = other.start
        en = start
      }
      val distance:Period = Period.between(st, en)

      if (distance.isZero())
        true
      else {
        if (distance.get(resolution.unit) % resolution.get(resolution.unit) == 0)
          true
        else
          false
      }
    } else
      false
  }

  lazy val resolution:Resolution = Resolution.between(start, end)
}

object Interval {
  def apply(mn: LocalDate, mx: LocalDate) =
   if(mn.isAfter(mx))
     throw new IllegalArgumentException("StartDate cannot be after end date")
   else
      new Interval(mn,mx)
}
