package edu.drexel.cs.dbgroup.temporalgraph.util

import scala.reflect.ClassTag
import java.time.LocalDate
import java.sql.Date

import org.apache.spark.rdd.RDD

import edu.drexel.cs.dbgroup.temporalgraph.Interval

object TempGraphOps extends Serializable {
  def dateOrdering: Ordering[LocalDate] = Ordering.fromLessThan((a,b) => a.isBefore(b))

  def minDate(a: LocalDate, b: LocalDate): LocalDate = if (a.isBefore(b)) a else b
  def maxDate(a: LocalDate, b: LocalDate): LocalDate = if (a.isBefore(b)) b else a

  implicit def dateWrapper(dt: LocalDate): Date = Date.valueOf(dt)

  def intervalUnion(intervals: Seq[Interval], other: Seq[Interval]): Seq[Interval] = {
    val spanend = intervals.last.end
    implicit val ord = dateOrdering
    intervals.map(in => in.start)
      .union(intervals.map(in => in.end))
      .union(other.map(in => in.end))
      .union(other.map(in => in.start))
      .sortBy(c => c)
      .distinct
      .sliding(2)
      .map(x => Interval(x(0), x(1)))
      .toSeq
  }

  def intervalIntersect(intervals: Seq[Interval], other: Seq[Interval]): Seq[Interval] = {
    val st: LocalDate = maxDate(intervals.head.start, other.head.start)
    val en: LocalDate = minDate(intervals.last.end, other.last.end)
    implicit val ord = dateOrdering
    (intervals.dropWhile(in => in.start.isBefore(st)).map(in => in.start)
      .union(other.dropWhile(in => in.start.isBefore(st)).map(in => in.start))
      .sortBy(c => c)
      .distinct
      .takeWhile(c => c.isBefore(en))
      :+ en)    
      .sliding(2)
      .map(x => Interval(x(0), x(1)))
      .toSeq
  }
  
}
