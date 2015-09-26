package edu.drexel.cs.dbgroup.graphxt.util

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import java.time.LocalDate

object DateFileFilter {
  private var minDate = LocalDate.now
  private var maxDate = LocalDate.now

  def setMinDate(min:LocalDate):Unit = minDate = min
  def setMaxDate(max:LocalDate):Unit = maxDate = max
}

class DateFileFilter extends PathFilter  {
  override def accept(path:Path):Boolean = {
    //extract the date
    val dt = LocalDate.parse(path.getName().dropWhile(!_.isDigit).takeWhile(_ != '.'))
    if (dt.isAfter(DateFileFilter.maxDate) || dt.isBefore(DateFileFilter.minDate))
      false
    else
      true    
  }
}

