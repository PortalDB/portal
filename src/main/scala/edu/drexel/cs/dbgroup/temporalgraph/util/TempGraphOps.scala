package edu.drexel.cs.dbgroup.temporalgraph.util

import scala.reflect.ClassTag
import java.time.LocalDate
import java.sql.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._

import edu.drexel.cs.dbgroup.temporalgraph.{Interval,ProgramContext,Resolution}

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


  def intervalDifference(intervals: RDD[Interval], other: RDD[Interval]): RDD[Interval] = {
    val st: LocalDate = intervals.min.start
    val en: LocalDate = intervals.max.end
    val spanend = intervals.max.end
    implicit val ord = dateOrdering
    intervals.map(in => in.start)
      .union(intervals.map(in => in.end))
      .union(other.filter(in => !in.start.isBefore(st) && in.start.isBefore(en)).map(_.start))
      .union(other.filter(in => !in.end.isBefore(st) && in.start.isBefore(en)).map(_.end))
      .distinct
      .sortBy(c => c, true)
      .sliding(2)
      .map(x => Interval(x(0), x(1)))
    /*
    val intersect=intervalIntersect(intervals,other)

    val st: LocalDate = intervals.min.start
    val en: LocalDate = intervals.max.end
    val intv = Interval(st, en)
    implicit val ord = dateOrdering


    val difference=intervals.map(_.start)


      .union(other.filter(in => !in.start.isBefore(st) && in.start.isBefore(en)).map(_.start))
      .union(other.filter(in => in.end.isAfter(st) && !in.start.isAfter(en)).map(_.end))
      .union(ProgramContext.sc.parallelize(Seq(en)))
      .distinct
      .sortBy(c => c, true)
      .sliding(2)
      .map(x => Interval(x(0), x(1)))
    difference.subtract(intersect).distinct().sortBy(c=>c,true)
    */

    /*
    val st: LocalDate = maxDate(intervals.min.start, other.min.start)
    val en: LocalDate = minDate(intervals.max.end, other.max.end)
    val intv = Interval(st, en)
    implicit val ord = dateOrdering
    intervals.map(_.start)
      .union(other.filter(in => !in.start.isBefore(st) && in.start.isBefore(en)).map(_.start))
      .union(ProgramContext.sc.parallelize(Seq(en)))
      .distinct
      .sortBy(c => c, true)
      .sliding(2)
      .map(x => Interval(x(0), x(1)))
      */

  }


  def combine(lst: List[Interval]): List[Interval] = {
    implicit val ord = dateOrdering
    lst.sortBy(x => x.start).foldLeft(List[Interval]()){ (r,c) => r match {
      case head :: tail =>
        if (head.intersects(c)) Interval(head.start, TempGraphOps.maxDate(head.end, c.end)) :: tail else c :: head :: tail
      case Nil => List(c)
    }}
  }

  /*
   * Take the map of intervals to values at those intervals and return an ordered list of values at regular
   * intervals (i.e. monthly or yearly, etc., depending on the resolution of the data),
   * filling in missing spots with provided fill value. If fillValue is None, then there's essentialy no filling.
  */
  def makeSeries[K: ClassTag](map: Map[Interval,K], fillValue: Option[K] = None): IndexedSeq[Option[K]] = {
    val unit = map.keys.map{ intv => Resolution.between(intv.start, intv.end).unit }
      .reduce( (x,y) => if (x.compareTo(y) < 0) x else y)

    //get the smallest date as start
    val st = map.keys.map(ii => ii.start).reduce((x,y) => if (x.isBefore(y)) x else y)

    //now turn intervals into points by unit
    val remap = map.flatMap{ case (k,v) =>
      val inst = unit.between(st, k.start).toInt
      val inen = unit.between(st, k.end).toInt
      (inst to inen).map(ii => (ii, v))
    }

    //now we have have a map of indices to values, but it's unordered and may have holes
    (remap.keys.min to remap.keys.max).map(x => if (remap.contains(x)) remap.get(x) else fillValue)
  } 
  
}
