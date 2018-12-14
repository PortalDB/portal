package edu.drexel.cs.dbgroup.portal.util

/* Implements the basic temporal algebra operations
 * over RDDs that simulate temporal relations
 */

import java.time.LocalDate
import scala.reflect.ClassTag
import scala.collection.mutable.Builder

import org.apache.spark.rdd.RDD

import edu.drexel.cs.dbgroup.portal.Interval

object TemporalAlgebra {
  //tuples within the same group have to be broken up by intervals
  //to create matching groups
  val combOp = (a: Set[LocalDate], b: Set[LocalDate]) => {
    a ++ b
/*
    implicit val ord = new Ordering[LocalDate] {
      override def compare(x: LocalDate, y: LocalDate): Int = {
        if (x.isBefore(y))
          -1
        else if (y.isBefore(x))
          1
        else 
          0
      }
    }
    def rec(x: Seq[LocalDate], y: Seq[LocalDate], acc: Builder[LocalDate, Seq[LocalDate]]): Builder[LocalDate, Seq[LocalDate]] = {
      (x, y) match {
        case (Nil, Nil) => acc
        case (_, Nil)   => acc ++= x
        case (Nil, _)   => acc ++= y
        case (xh :: xt, yh :: yt) =>
          if (ord.equiv(xh, yh))
            rec(xt, yt, acc += xh)
          else if (ord.lt(xh, yh))
            rec(xt, y, acc += xh)
          else
            rec(x, yt, acc += yh)
      }
    }
    rec(a, b, Seq.newBuilder).result
*/
  }

  val combOpL = (a: List[Interval], b: List[Interval]) => {
    //a and b are sorted and each element is non-overlapping
    if (a.size == 0)
      b
    else if (b.size == 0)
      a
    else {
      implicit val ord = TempGraphOps.dateOrdering
      def rec(x: List[LocalDate], y: List[LocalDate], acc: Builder[LocalDate, List[LocalDate]]): Builder[LocalDate, List[LocalDate]] = {
        (x, y) match {
          case (Nil, Nil) => acc
          case (_, Nil)   => acc ++= x
          case (Nil, _)   => acc ++= y
          case (xh :: xt, yh :: yt) =>
            if (ord.lteq(xh, yh))
              rec(xt, y, acc += xh)
            else
              rec(x, yt, acc += yh)
        }
      }
      rec(a.flatMap(x => List(x.start, x.end)), b.flatMap(x => List(x.start, x.end)), List.newBuilder).result
/*
        (a.flatMap(x => Seq(x.start, x.end)) ++ b.flatMap(x => Seq(x.start, x.end)))
        .sortBy(x => x)
 */
        .foldLeft(List[Interval](Interval(LocalDate.MIN, LocalDate.MIN)))({
          case (a,b) =>
            if (!a.head.end.equals(b))
              Interval(a.head.end, b) :: a
            else
              a
        }).reverse.tail.tail

    }
  }

  def select[T: ClassTag](rel: RDD[(T, Interval)], f: (T, Interval) => Boolean): RDD[(T, Interval)] = {
    //this one does not require anything special, just a pass through
    rel.filter(x => f(x._1, x._2))
  }

  /*
   * This combines map and group by, but also temporal
   */
  def aggregate[T: ClassTag, K: ClassTag, V: ClassTag](rel: RDD[(T, Interval)], groupBy: T => K, seqFunc: (T, V) => V, aggFunc: (V, V) => V, zeroVal: V): RDD[((K, Interval), V)] = {
    val intervals: RDD[(K, List[Interval])] = rel.map(x => (groupBy(x._1), x._2)).aggregateByKey(List[Interval]())(seqOp = (u: List[Interval], v: Interval) => TemporalAlgebra.combOpL(u, List[Interval](v)), TemporalAlgebra.combOpL)

    rel.map{ case (x, intv) => (groupBy(x), (intv, seqFunc(x, zeroVal)))}
      .join(intervals)
      .flatMap{ case (k,v) => v._2.filter(x => x.intersects(v._1._1)).map(x => ((k, x), v._1._2))}
      .reduceByKey(aggFunc)
      .map(x => ((x._1._1, x._1._2), x._2))

  }

  def union[K: ClassTag, V: ClassTag](one: RDD[(K, (Interval, V))], two: RDD[(K, (Interval, V))]): RDD[((K, Interval), V)] = {
    val un = one.union(two)
    val intervals: RDD[(K, List[Interval])] = un.aggregateByKey(List[Interval]())(seqOp = (u: List[Interval], v: (Interval, V)) => TemporalAlgebra.combOpL(u, List[Interval](v._1)), TemporalAlgebra.combOpL)

    un.join(intervals)
      .flatMap{ case (k,v) => v._2.filter(x => x.intersects(v._1._1)).map(x => ((k,x), v._1._2))}
  }

}
