package edu.drexel.cs.dbgroup.graphxt

import scala.math.Ordered.orderingToOrdered
import scala.math.Ordering._

class Interval(mn: TimeIndex, mx: TimeIndex) extends Ordered[Interval] with Serializable {
  var min = mn
  var max = mx

  def contains(other: Interval):Boolean = {
    if (other.min >= min && other.max <= max)
      true
    else
      false
  }

  def contains(num: TimeIndex):Boolean = {
    if (num >= min && num <= max)
      true
    else
      false
  }

  def compare(other: Interval): Int = {
    (this.min, this.max) compare (other.min, other.max)
  }

  //if the other interval has any (including complete) overlap in years, return true
  def intersects(other: Interval):Boolean = {
    if (other.min > max || other.max < min)
      false
    else
      true
  }
}

object Interval {
  def apply(mn: TimeIndex, mx: TimeIndex) = new Interval(mn,mx)
}
