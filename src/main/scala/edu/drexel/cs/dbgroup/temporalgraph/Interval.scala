package edu.drexel.cs.dbgroup.temporalgraph

import scala.math.Ordering._
import java.time.LocalDate
import java.sql.Date
import java.time.temporal.ChronoUnit
import org.apache.spark.sql.catalyst.util.DateTimeUtils

import edu.drexel.cs.dbgroup.temporalgraph.util.TempGraphOps._

/**
  * Time period with a closed-open model, i.e. [start, end)
  * A time period where start=end is null/empty.
  */
class Interval(st: LocalDate, en: LocalDate) extends Serializable {
  val start:LocalDate = st
  val end:LocalDate = en

  def getStartSeconds:Long = math.floor(DateTimeUtils.daysToMillis(start.toEpochDay().toInt).toDouble / 1000L).toLong
  def getEndSeconds:Long = math.floor(DateTimeUtils.daysToMillis(end.toEpochDay().toInt).toDouble / 1000L).toLong

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

  //like intersects, but returns true if start/end are equal
  def touches(other: Interval):Boolean = {
    if(other.start.isAfter(end) || other.end.isBefore(start))
      false
    else
      true
  }

  def union(other: Interval): Interval = Interval(minDate(start, other.start), maxDate(end, other.end))

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

  /*
   * Return the portions of this interval that are not included in the other
   */
  def difference(other: Interval): List[Interval] = {
    if (intersects(other)) {
      var res = List[Interval]()
      if (start.isBefore(other.start)) res = res :+ Interval(start, other.start)
      if (end.isAfter(other.end)) res = res :+ Interval(other.end, end)
      res
    } else List(this)
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
   */
  def split(period: Resolution, mark: LocalDate): Seq[(Interval, Interval)] = {
    if(mark.isAfter(this.start))
      throw new IllegalArgumentException("markDate cannot be after interval start date")
    var res: List[(Interval,Interval)] = List()
    var markStart: LocalDate = start
    var markEnd: LocalDate = mark

    while (end.isAfter(markEnd)) {
      val step = period.getInterval(markEnd)
      if (markStart.isBefore(step.end)) {
        val nextIntv: Interval = if (markStart == step.start && end == step.end) step else Interval(markStart, minDate(step.end, end))
        res = (nextIntv, step) :: res
        markStart = step.end
      }
      markEnd = step.end
    }

    res
  }

}

object Interval {
  implicit def ordering: Ordering[Interval] = new Ordering[Interval] {
    override def compare(a: Interval, b: Interval): Int = { a compare b }
  }

  final val SECONDS_PER_DAY = 60 * 60 * 24L

  def apply(mn: LocalDate, mx: LocalDate) =
   if(mn.isAfter(mx))
     throw new IllegalArgumentException("StartDate cannot be after end date")
   else
      new Interval(mn,mx)

  def apply(mn: Date, mx: Date) = 
    if (mn.after(mx))
      throw new IllegalArgumentException("Start date cannot be after end date")
    else
      new Interval(mn.toLocalDate(), mx.toLocalDate())

  //inputs are seconds since 1970
  def apply(mn: Long, mx: Long) = 
    if (mn > mx)
      throw new IllegalArgumentException("Start date cannot be after end date")
    else
      new Interval(LocalDate.ofEpochDay(math.floor(mn.toDouble/SECONDS_PER_DAY).toLong), LocalDate.ofEpochDay(math.floor(mx.toDouble / SECONDS_PER_DAY).toLong))

  def empty = new Interval(LocalDate.MAX, LocalDate.MAX)

  //TODO: need to add unit test for this
  def parse(str: String): Interval = {
    try {
      val dates = str.split("-").map(x => x.toInt)
      new Interval(LocalDate.of(dates(0),dates(1),dates(2)),LocalDate.of(dates(3),dates(4),dates(5)))
    } catch {
      case _ : Throwable =>
      throw new IllegalArgumentException("Invalid interval string. Format:yyy-mm-dd-yyyy-mm-dd")
    }    
  }

  def applyOption(mn: LocalDate, mx: LocalDate) : Option[Interval] =
    if(mn.after(mx) || mn.equals(mx))
      None
    else
      Some(new Interval(mn,mx))

  /**
    * gets the difference between an interval and another list of intervals.
    * @param vertexInterval should CONTAIN every one of the edgeIntervals
    * @param edgeIntervals each one should be CONTAINED BY vertexInterval
    * @return
    */
  def differenceList(vertexInterval: Interval, edgeIntervals: List[Interval]) : List[Interval] = {
    var returnVal = List[Interval]()
    if(edgeIntervals.size == 0) returnVal = returnVal
    else if(edgeIntervals.size == 1) returnVal = vertexInterval.difference(edgeIntervals(0))
    else{
      var coalescedEdgeIntervals = Interval.coalesce(edgeIntervals)
      var i = 0
      while(i < coalescedEdgeIntervals.size){
        val prevInterval = if(i == 0) None else Some[Interval](coalescedEdgeIntervals(i-1))
        val thisInterval = coalescedEdgeIntervals(i)
        val nextInterval = if(i+1 < coalescedEdgeIntervals.size) Some[Interval](coalescedEdgeIntervals(i+1)) else None
        //beginning
        if(prevInterval == None){
          Interval.applyOption(vertexInterval.start,thisInterval.start) +: returnVal
        }
        //end
        else if(nextInterval == None){
          List[Interval](Interval.applyOption(prevInterval.get.end,thisInterval.start).get,
            Interval.applyOption(thisInterval.end,vertexInterval.end).get) ::: returnVal
        }
        //middle
        else{
          Interval.applyOption(prevInterval.get.start,thisInterval.start) +: returnVal
        }
        i += 1
      }
    }
    returnVal
  }

  /**
    * given a list of intervals, produces the sorted list of non-intersecting, non-touching intervals
    * @param original
    * @return
    */
  def coalesce(original: List[Interval]) : List[Interval] = {
    var res = List[Interval]()
    val srt = original.sorted
    val fld = srt.foldLeft(List[Interval]()){(list,elem) =>
      list match{
        case Nil => List(elem)
        case head :: tail if(!head.touches(elem)) => elem :: head :: tail
        case head :: tail => {
          val newStart = if(head.start.compareTo(elem.start) < 0) head.start else elem.start
          val newEnd = if(head.end.compareTo(elem.end) > 0) head.end else elem.end
          Interval.apply(newStart,newEnd) :: tail
        }
      }
    }
    fld.sorted
  }
}
