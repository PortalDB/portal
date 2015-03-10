package edu.drexel.cs.dbgroup.graphxt

class Interval(mn: Int, mx: Int) extends Serializable {
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

  //returns true if this interval is smaller (i.e. starts earlier) than other other
  def <(other: Interval):Boolean = {
    if (min < other.min)
      true
    else
      false
  }

  //if the other interval has any (including complete) overlap in years, return true
  def intersects(other: Interval):Boolean = {
    if (other.min > max || other.max < min)
      false
    else
      true
  }
}

