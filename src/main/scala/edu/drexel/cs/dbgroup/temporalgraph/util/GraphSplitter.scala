package edu.drexel.cs.dbgroup.temporalgraph.util

import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark._

import edu.drexel.cs.dbgroup.temporalgraph._

class GraphSplitter(rdd: RDD[Interval]) {

  private val N: Long = rdd.count

  private val starts: Array[LocalDate] = {
    implicit val ord = TempGraphOps.dateOrdering
    rdd.map(x => x.start).distinct.sortBy(x => x).collect
  }

  private val counts: Array[(LocalDate, Long, Long)] = {
    val startsb = ProgramContext.sc.broadcast(starts)
    val pcounts = rdd.flatMap { intv =>
      val st = startsb.value.dropWhile(x => x.isBefore(intv.start) || x.equals(intv.start))
      val en = startsb.value.dropWhile(x => x.isBefore(intv.end))
      if (st.size > 0) {
        if (en.size > 0) {
          if (st.head.equals(en.head))
            Some((st.head, (1L, 1L)))
          else
            List((st.head, (1L, 0L)), (en.head, (0L, 1L)))
        } else 
          Some((st.head, (1L, 0L)))
      } else if (en.size > 0) {
        Some((en.head, (0L, 1L)))
      } else
        None
    }.reduceByKey((a,b) => (a._1+b._1, a._2+b._2)).collectAsMap
    starts.map{x => val y = pcounts.get(x).getOrElse((0L, 0L)); (x, y._1, y._2)}.scanLeft((starts.head, 0L, 0L))((a,b) => (b._1, a._2+b._2, a._3+b._3)).tail
  }

    /**
    * Given a desired number of splits and the maximum allowed cost,
    * return the actual cost and the splits.
    * @param k The number of splits (number of buckets is thus k+1)
    * @param t The maximum allowed cost, i.e. number of items per split
    * @return cost Actual biggest number of items across all buckets.
    * 0 cost means no split with these constraints is possible.
    * @return splits The array of dates that split into buckets
    */
  def getSplit(k: Int, t: Long): (Long, Array[LocalDate]) = {
    if (t >= N)
      return (N, Array[LocalDate]())
    val dates: Array[LocalDate] = new Array(k+1)
    var tmax: Long = 0L
    var l: Int = 0 //number of dates so far
    var previous: Long = 0L

    import scala.util.control.Breaks._
    breakable {for (j <- 1 to counts.size-1) {
      val x = counts(j)
      if (x._2 - previous > t) {
        val d = counts(j-1)
        if ((l == 0 && j == 1) || (l > 0 && dates(l-1).equals(d._1)))
          return (0, Array[LocalDate]())
        tmax = math.max(tmax, d._2 - previous)
        previous = d._3
        dates(l) = d._1
        l += 1
        if (l == k)
          break

      }
    } }

    if (l < k && (N-previous) > t) {
      val x = counts.last
      dates(l) = x._1
      l += 1
      previous = x._3
    }

    val b = N - previous
    if (b > t) return (0, Array[LocalDate]())

    (math.max(tmax,b), dates.take(l))

  }

  /*
   * Find the split with the smallest cost for this k
   * @param k The number of splits. The number of buckets is k+1
   */
  def findSplit(k: Int, max: Long = N): (Long, Array[LocalDate]) = {
    //use binary search between N-1 and N/(k+1)
    var best: Long = N
    var split: Array[LocalDate] = Array()
    var bottom: Long = N/(k+1)
    var top: Long = max
    var last: Long = 0
    while (bottom < top) {
      val stab: Long = math.floor((bottom + top) / 2).toLong
      if (stab == last) {
        bottom = top
      } else {
        last = stab
        val (c, sp) = getSplit(k, stab)
        if (c > 0) {
          best = c
          split = sp
          top = stab
        } else {
          bottom = stab
        }
      }
    }

    (best, split)
  }

}
