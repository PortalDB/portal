package edu.drexel.cs.dbgroup.portal.util

import java.time.LocalDate
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.Lock
import java.util.concurrent.CountDownLatch
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark._

import edu.drexel.cs.dbgroup.portal._

/**
  this is the a parallelized method still using the stabbing count array
  cost of constructing stabbing count array
      W(n) = theta(n log n) 
      S(n) = theta(log n)
      P(n) = O(n)

  cost of finding solution with a given k
      W(n) = logM(N)*cMk  (h = cM)
      S(n) = logM(N)
      P(n) = cMk
*/

class SplitterParallel(rdd: RDD[Interval]) {

  private val stabbingArray: RDD[(Long, (Long,Long,LocalDate))] = {
    //first need to sort the rdd based on start date
    implicit val ord = EndPointsOrdering
    val sorted: RDD[(LocalDate, Long, LocalDate)] = rdd.zipWithIndex().flatMap(x => List((x._1.start, x._2, x._1.start),(x._1.end, x._2, x._1.start))).sortBy(x => x)

    //we compute the counters locally for each partition
    //and then apply that data again for each partition to combine
    val partData: RDD[(Int, (Long, Long, Long, LocalDate))] = sorted.mapPartitionsWithIndex({ (index, iter) =>
      var c: Long = 0
      var cd: Long = 0
      var slast: LocalDate = LocalDate.MIN
      var x: Long = 0L
      iter.foreach { nel =>
        //starting value?
        if (nel._1.equals(nel._3)) {
          c = c + 1
          if (nel._1.equals(slast))
            cd = cd + 1
          else {
            slast = nel._1
            cd = 1
          }
          x += 1
        } else { //ending value
          c = c - 1
          if (slast.equals(nel._3)) cd = cd - 1
        }
      }
      Iterator((index, (x, c, cd, slast)))
    })

    val fullPartData: Array[(Long, Long, Long, LocalDate)] = partData.collect().sortBy(x => x._1).map(x => x._2).scanLeft((0L, 0L, 0L, LocalDate.MIN)){(a,b) => 
      if (a._4.equals(b._4)) 
        (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4) 
      else if (a._4.isAfter(b._4))
        (a._1 + b._1, a._2 + b._2, a._3, a._4)
      else
        (a._1 + b._1, a._2 + b._2, b._3, b._4)
    }.tail

    sorted.mapPartitionsWithIndex({ (index, iter) =>
      val rolling = if (index > 0) fullPartData(index-1) else (0L, 0L, 0L, LocalDate.MIN)
      var x: Long = rolling._1
      var c: Long = rolling._2
      var cd: Long = rolling._3
      var slast: LocalDate = rolling._4
      val buf= new ArrayBuffer[(Long, (Long, Long, LocalDate))]

      iter.foreach { nel =>
        //starting value?
        if (nel._1.equals(nel._3)) {
          c = c + 1
          if (nel._1.equals(slast))
            cd = cd + 1
          else {
            slast = nel._1
            cd = 1
          }
          buf += ((x, (c - cd, cd - 1, nel._1)))
          x += 1
        } else { //ending value
          c = c - 1
          if (slast.equals(nel._3)) cd = cd - 1
        }
      }
      buf.toIterator
    }).cache

  }
 
  private val N: Long = stabbingArray.count

  //largest index in each partition by partition index
  private val max: Array[Long] = stabbingArray.mapPartitionsWithIndex({ (idx, iter) => 
    Iterator((idx, iter.maxBy(x => x._1)._1))
  }).collect.sortBy(x => x._1).map(x => x._2)

  /*
   * Find the split with the smallest cost for this k
   * @param k The number of splits. The number of buckets is k+1
   */
  def findSplit(k: Int, max: Long = N): (Long, Array[LocalDate]) = {
    //search with testing several=h at the same time
    val h = 8
    //pick h equi-spaced values of t, test all of them together
    var bottom: Long = N/(k+1)
    var top: Long = max
    var lowest: Long = 0L
    while (bottom <= top) {
      val step = math.max(1,(top - bottom)/h)
      val tries = bottom to top by step
      val res = testSplit(k, tries)
      if (res > 0) {
        lowest = res
        top = lowest-1
        bottom = math.max(bottom, lowest - step)+1
      } else {
        bottom = tries.last+1
      }
    }

    //now that we know which t works, retrieve the split
    if (lowest > 0)
      getSplit(k, lowest)
    else
      (0, Array[LocalDate]())
  }

  /**
    * Given a desired number of splits and a set of maximum allowed costs,
    * return the smallest of allowed that results in a split, or 0 otherwise.
    * @param k The number of splits (number of buckets is thus k+1)
    * @param ts The set of maximum allowed costs, i.e. number of items per split
    * assumes the list is sorted from smallest to largest and no repeats
    * @return cost Smallest of allowed costs that gives a valid split.
    * 0 cost means no split with these constraints is possible.
    */
  def testSplit(k: Int, ts: Seq[Long]): Long = {
    val index = max.indexWhere(x => x >= ts.head)
    if (index < 0)
      return ts.head

    //start ts.size number of splitter threads
    val latch: CountDownLatch = new CountDownLatch(ts.size)
    val stc = new SynchronizedStabbingCount(max, stabbingArray, N, latch)
    val splitters = ts.map { x =>
      new SplitterRunnable(k, x, N, stc)
    }
    splitters.foreach { x => x.start() }

    //now start one thread that will fetch partitions when needed
    val producer = new Thread(new Producer(stc, N))
    producer.start()

    //TODO: if one splitter returned with success, then all
    //larger splitters can be stopped (since we want the smallest)

    latch.await()
    //this is a bit of a hack to signal the producer it's done
    //FIXME: do this more properly
    stc.top = N 
    //now all splitters finished, get the lowest t that got true
    val lowest = splitters.map{ x => x.getValue }.filter(x => x._2 == true)
    if (lowest.size > 0)
      lowest.head._1
    else
      0L
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
    val points: Array[Long] = new Array(k)
    var lookup = stabbingArray.lookup(t).head
    points(0) = t + 1 - lookup._2
    var b: Long = t - lookup._2
    if (b == 0)
      return (0, Array[LocalDate]())
    var tmax: Long = b

    for (j <- 1 to k-1) {
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
}

class SynchronizedStabbingCount(max: Array[Long], stabbingArray: RDD[(Long, (Long,Long,LocalDate))], N: Long, latch: CountDownLatch) {
  //TODO: if the stabbing count array is small enough to fit into the driver
  //memory, just fetch the whole thing once
  private val minHeap = scala.collection.mutable.PriorityQueue.empty(Ordering[Long].reverse)
  private var points: Array[(Long, Long, LocalDate)] = new Array[(Long, Long, LocalDate)](0)
  private var bottom: Long = 0L
  var top: Long = -1L
  private val aLock: Lock = new java.util.concurrent.locks.ReentrantLock()
  private val nextBlock: Condition = aLock.newCondition()
  private val queueCond: Condition = aLock.newCondition()

  def done(): Unit = {
    aLock.lock
    try {
      latch.countDown()
      if (latch.getCount >= minHeap.size)
        queueCond.signal()
    } finally {
      aLock.unlock()
    }
  }

  def get(t: Long): (Long,Long,LocalDate) = {
    aLock.lock
    try {
      minHeap.enqueue(t)

      while (t > top) {
        if (minHeap.size == latch.getCount)
          queueCond.signal()
        nextBlock.await()
      }

      minHeap.dequeue
      points((t - bottom).toInt)

    } finally {
      aLock.unlock()
    }
  }

  def put: Unit = {
    aLock.lock
    try {
      //dequeue the smallest t requested
      while (minHeap.size < latch.getCount) {
        queueCond.await()
      }

      if (latch.getCount == 0) return

      val req = minHeap.head
      if (req > top) {
        val index = max.indexWhere(x => x >= req)
        if (index < 0) //should never happen, something bad occurred
          throw new InterruptedException("could not find partition for value " + req)
        points = stabbingArray.mapPartitionsWithIndex((idx, iter) => if (idx == index) iter else Iterator()).collect().sortBy(x => x._1).map(x => x._2)
        bottom = max.lift(index-1).getOrElse(-1L) + 1
        top = max(index)
        nextBlock.signalAll()
      }
    } finally {
      aLock.unlock()
    }
  }

}

class Producer(sta: SynchronizedStabbingCount, N: Long) extends Runnable {
  override def run(): Unit = {
    try {
      while (sta.top < N) {
        sta.put
      }
    } finally { 
    }
  }
}

class SplitterRunnable(k: Int, t: Long, N: Long, sta: SynchronizedStabbingCount) extends Thread {
  private var value: (Long, Boolean) = (t, false)

  override def run(): Unit = {
    if (t >= N) {
      value = (t, true)
      sta.done
      return
    }

    var lookup = sta.get(t)
    var point: Long = t + 1 - lookup._2
    var b: Long = t - lookup._2
    if (b == 0) {
      value = (t, false)
      sta.done
      return
    }

    for (j <- 1 to k-1) {
      val xj = point + t - lookup._1
      if (xj > N) {
        value = (t, true)
        sta.done
        return
      }
      lookup = sta.get(xj-1)
      val newpoint = xj - lookup._2
      if (newpoint == point) {
        value = (t, false)
        sta.done
        return
      }
      point = newpoint
    }
    b = N - point + 1 + lookup._1
    if (b > t) {
      value = (t, false)
      sta.done
      return
    }

    value = (t, true)
    sta.done()
  }

  def getValue: (Long, Boolean) = value

}
