package edu.drexel.cs.dbgroup.graphxt

import scala.math.Ordered.orderingToOrdered

trait Serializable

class Interval(mn: Int, mx: Int) extends Ordered[Interval] with Serializable {
  var min = mn
  var max = mx

  def contains(other: Interval):Boolean = {
    if (other.min >= min && other.max <= max)
      true
    else
      false
  }

  def contains(num: Int):Boolean = {
    if (num >= min && num <= max)
      true
    else
      false
  }

  //TODO: is this needed when we extend Ordered and define compare()
  //returns true if this interval is smaller (i.e. starts earlier) than other other
//  def <(other: Interval):Boolean = {
//    if (min < other.min)
//      true
//    else
//      false
//  }
//  
//  //returns true if this interval is larger (i.e. starts later) than other other
//  def >(other: Interval):Boolean = {
//    if (max > other.max)
//      true
//    else
//      false
//  }
  
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

