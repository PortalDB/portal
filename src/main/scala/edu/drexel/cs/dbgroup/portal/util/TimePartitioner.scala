package edu.drexel.cs.dbgroup.portal.util

import scala.reflect.ClassTag
import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner

import edu.drexel.cs.dbgroup.portal.{Interval,ProgramContext}

class TimePartitioner(partitions: Int, buckets: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")
  //partitions is a multiple of buckets
  private val parts: Int = {
    val rem = partitions % buckets
    if (rem == 0)
      partitions
    else
      partitions + buckets - rem
  }
  private val partsPerBucket: Int = numPartitions / buckets
  def numPartitions: Int = parts
  def numBuckets: Int = buckets

  //FIXME: how can we check that this element is from the same rdd
  //as what it was computed for?
  def getPartition(key: Any): Int = {
    val i: (Int, Any) = key.asInstanceOf[(Int,Any)]
    if (partsPerBucket > 1) {
      val start = i._1*partsPerBucket
      val rawMod = i._2.hashCode % partsPerBucket
      start + rawMod + (if (rawMod < 0) partsPerBucket else 0)
    } else i._1
  }

  override def equals(other: Any): Boolean = other match {
    case h: TimePartitioner =>
      h.numPartitions == numPartitions && h.numBuckets == buckets
    case _ =>
      false
  }

  override def hashCode: Int = (partitions, buckets).hashCode

}

object TimePartitioner {

  //given an RDD with a time attribute
  //partition by time, splitting as necessary
  //unfortunately we have to split into multiple rdds or otherwise
  //computations downstream will be wrong
  def partition[T: ClassTag](rdd: RDD[(T,Interval)]): Seq[RDD[(T,Interval)]] = {
    //how many buckets do we want?
    //let's do as many as there are executors
    val executors = 3//ProgramContext.sc.getExecutorMemoryStatus.size
    println("number of executors running: " + executors)
    if (executors == 1) //local mode, no sense to split
      return Seq(rdd)

    //compute the splitters
    val splitter = new GraphSplitter(rdd.map(_._2))
    val (c, splitters) = splitter.findSplit(executors - 1)
    println("split into " + splitters.size + " splitters")
    if (c == 0) { //could not split
      println("unable to find a split, returning as is")
      return Seq(rdd)
    }

    val splitB = ProgramContext.sc.broadcast((LocalDate.MIN +: splitters :+ LocalDate.MAX).sliding(2).map(x => Interval(x(0),x(1))).zipWithIndex.toSeq)
    println("computed buckets: " + splitB.value.mkString("; "))

    //now split according to the splitters
    //note: the number of splitters may be smaller than executors-1
    //each bucket gives one rdd

/**
  * don't split tuples. place into the first bucket that matches
  * this does not increase max load per bucket but does worsen
  * communication load
*/
    val split: RDD[((Int,T),Interval)] = rdd.mapPartitions({iter =>
      val splitters = splitB.value
      iter.map(x => ((splitters.dropWhile(y => !y._1.intersects(x._2)).head._2,x._1), x._2))
    })

    //each partition has to fit within executor memory
    //which means we may have to allocate multiple partitions to a bucket
    //we cannot use hashpartitioner if the rdd has a lot of partitions
    Seq(split.partitionBy(new TimePartitioner(split.getNumPartitions, executors))
      .mapPartitions({ iter => iter.map(x => (x._1._2, x._2))}))

/**
  * split into multiple rdd method
  * issue: produces incorrect results with aggregation
  * because of insufficient information about hanging interval groups

    val mux = rdd.mapPartitionsWithIndex { case (id, itr) =>
      val buckets = Vector.fill(splitB.value.size) { ArrayBuffer.empty[(T, Interval)] }
      itr.foreach { e => splitB.value.filter(y => y._1.intersects(e._2)).foreach(y => buckets(y._2) += e)}
      Iterator.single(buckets)
    }

    Vector.tabulate(splitB.value.size) { j => mux.mapPartitions { itr => itr.next()(j).toIterator }}

    //TODO: need to coalesce each rdd into total/buckets partitions
*/

/**
  * split tuples (keeping timestamp) and partition by bucket
  * issue: produces incorrect results because split tuples
  * are counted as many times as they are split

    val split: RDD[((Int,T),Interval)] = rdd.flatMap(x => splitB.value.filter(y => y._1.intersects(x._2)).map(y => ((y._2, x._1), x._2)))

    //each partition has to fit within executor memory
    //which means we may have to allocate multiple partitions to a bucket
    //we cannot use hashpartitioner if the rdd has a lot of partitions
    split.partitionBy(new TimePartitioner(split.getNumPartitions, executors))
      .mapPartitions({ iter => iter.map(x => (x._1._2, x._2))})
*/
  }

}
