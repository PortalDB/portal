package edu.drexel.cs.dbgroup.temporalgraph.util

import scala.reflect.ClassTag
import java.time.LocalDate

import org.apache.spark.rdd.RDD

import edu.drexel.cs.dbgroup.temporalgraph.Interval

object TempGraphOps {
  implicit def dateOrdering: Ordering[LocalDate] = Ordering.fromLessThan((a,b) => a.isBefore(b))

  implicit def minDate(a: LocalDate, b: LocalDate): LocalDate = if (a.isBefore(b)) a else b
  implicit def maxDate(a: LocalDate, b: LocalDate): LocalDate = if (a.isBefore(b)) b else a
  
}
