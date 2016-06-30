package edu.drexel.cs.dbgroup.temporalgraph.util

import scala.reflect.ClassTag
import java.time.LocalDate
import java.sql.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._

import edu.drexel.cs.dbgroup.temporalgraph.{Interval,ProgramContext}

object TempGraphOps extends Serializable {
  def dateOrdering: Ordering[LocalDate] = Ordering.fromLessThan((a,b) => a.isBefore(b))

  def minDate(a: LocalDate, b: LocalDate): LocalDate = if (a.isBefore(b)) a else b
  def maxDate(a: LocalDate, b: LocalDate): LocalDate = if (a.isBefore(b)) b else a
  def minDate(a: LocalDate, b: LocalDate, c: LocalDate): LocalDate = minDate(minDate(a,b),c)
  def maxDate(a: LocalDate, b: LocalDate, c: LocalDate): LocalDate = maxDate(maxDate(a,b),c)

  implicit def dateWrapper(dt: LocalDate): Date = Date.valueOf(dt)

  def intervalUnion(intervals: RDD[Interval], other: RDD[Interval]): RDD[Interval] = {
    val spanend = intervals.max.end
    implicit val ord = dateOrdering
    intervals.map(in => in.start)
      .union(intervals.map(in => in.end))
      .union(other.map(in => in.end))
      .union(other.map(in => in.start))
      .distinct
      .sortBy(c => c, true)
      .sliding(2)
      .map(x => Interval(x(0), x(1)))
  }

  def intervalIntersect(intervals: RDD[Interval], other: RDD[Interval]): RDD[Interval] = {
    val st: LocalDate = maxDate(intervals.min.start, other.min.start)
    val en: LocalDate = minDate(intervals.max.end, other.max.end)
    val intv = Interval(st, en)
    implicit val ord = dateOrdering
    intervals.filter(in => !in.start.isBefore(st) && in.start.isBefore(en)).map(_.start)
      .union(other.filter(in => !in.start.isBefore(st) && in.start.isBefore(en)).map(_.start))
      .union(ProgramContext.sc.parallelize(Seq(en)))
      .distinct
      .sortBy(c => c, true)
      .sliding(2)
      .map(x => Interval(x(0), x(1)))
  }

  def combine(lst: List[Interval]): List[Interval] = {
    implicit val ord = dateOrdering
    lst.sortBy(x => x.start).foldLeft(List[Interval]()){ (r,c) => r match {
      case head :: tail =>
        if (head.intersects(c)) Interval(head.start, TempGraphOps.maxDate(head.end, c.end)) :: tail else c :: head :: tail
      case Nil => List(c)
    }}
  }
  
}
