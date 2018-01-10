package edu.drexel.cs.dbgroup.portal.util

import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark._

import edu.drexel.cs.dbgroup.portal._

/**
  this is the original method from the paper, adapted for Spark
  sort is parallelized, but computing stabbing count array and finding solution are not
  cost of constructing stabbing count array
  W(n) = theta(n log n) + theta(n)     O(n log n)
  S(n) = theta(log^2 n) + theta(n)     O(n)
  P(n) =                               O(log n)
  cost of finding solution with a given k
  W(n) = k log n
  S(n) = k log n
  P(n) = 1
*/

class Splitter(rdd: RDD[Interval]) {

  private val stabbingArray: RDD[(Long, (Long,Long,LocalDate))] = {
    //first need to sort the rdd based on start date
    implicit val ord = EndPointsOrdering
    val sorted: RDD[(LocalDate, Long, LocalDate)] = rdd.zipWithIndex().flatMap(x => List((x._1.start, x._2, x._1.start),(x._1.end, x._2, x._1.start))).sortBy(x => x)
    var c: Long = 0
    var cd: Long = 0
    var slast: LocalDate = LocalDate.MIN

    var result: Array[(Long, (Long, Long, LocalDate))] = new Array[(Long, (Long, Long, LocalDate))](10000000)
    var x: Int = 0
    var y: Long = 0L
    var rddres: RDD[(Long, (Long, Long, LocalDate))] = ProgramContext.sc.emptyRDD
    sorted.cache.toLocalIterator.foreach { nel =>
      //starting value?
      if (nel._1.equals(nel._3)) {
        c = c + 1
        if (nel._1.equals(slast)) 
          cd = cd + 1
        else {
          slast = nel._1
          cd = 1
        }
        result(x) = (y, (c - cd, cd - 1, nel._1))
        x += 1
        y += 1
        if (x == 10000000) {
          val interim = ProgramContext.sc.parallelize(result.toSeq)
	  interim.count
          rddres = rddres.union(interim)
          result = new Array(10000000)
          x = 0
        }

      } else { //ending value
        c = c - 1
        if (slast.equals(nel._3)) cd = cd - 1
      }

    }

    if (x > 0)
      rddres = rddres.union(ProgramContext.sc.parallelize(result.take(x).toSeq))

    rddres.partitionBy(new HashPartitioner(rddres.getNumPartitions)).cache
  }
 
  private val N: Long = stabbingArray.count

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
    val points: Array[Long] = new Array(k)
    var lookup = stabbingArray.lookup(t).head
    points(0) = t + 1 - lookup._2
    var b: Long = t - lookup._2
    if (b == 0)
      return (0, Array[LocalDate]())
    var tmax: Long = b

    for (j <- 1 to k-1) {
      lookup = stabbingArray.lookup(points(j - 1)-1).head
      val xj = points(j - 1) + t - lookup._1
      if (xj > N) {
        b = N - points(j - 1) + 1 + lookup._1
        return (math.max(b, tmax), points.take(j).map(x => stabbingArray.lookup(x-1).head._3).toArray)
      }
      lookup = stabbingArray.lookup(xj-1).head
      points(j) = xj - lookup._2
      if (points(j) == points(j-1)) return (0, Array[LocalDate]())
      b = t - lookup._2
      tmax = math.max(tmax, b)
    }
    b = N - points(k-1) + 1 + stabbingArray.lookup(points(k-1)-1).head._1
    if (b > t) return (0, Array[LocalDate]())
    
    (math.max(tmax, b), points.map(x => stabbingArray.lookup(x-1).head._3))

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

object EndPointsOrdering extends Ordering[(LocalDate, Long, LocalDate)] {
  def compare(a: (LocalDate, Long, LocalDate), b: (LocalDate, Long, LocalDate)): Int = {
    if (a._1.isBefore(b._1))
      -1
    else if (b._1.isBefore(a._1))
      1
    else { //break ties
      //ending values go before starting values
      if (a._3.equals(a._1) && b._3.equals(b._1)) scala.math.Ordering.Long.compare(a._2, b._2) //this should never be 0
      else if (a._3.equals(a._1)) 1
      else if (b._3.equals(b._1)) -1
      else scala.math.Ordering.Long.compare(a._2, b._2)
    }
  }
}
