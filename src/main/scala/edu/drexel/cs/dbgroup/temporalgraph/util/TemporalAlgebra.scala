package edu.drexel.cs.dbgroup.temporalgraph.util

/* Implements the basic temporal algebra operations
 * over RDDs that simulate temporal relations
 */

import java.time.LocalDate
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import edu.drexel.cs.dbgroup.temporalgraph.Interval

object TemporalAlgebra {

  def select[T: ClassTag](rel: RDD[(T, Interval)], f: (T, Interval) => Boolean): RDD[(T, Interval)] = {
    //this one does not require anything special, just a pass through
    rel.filter(x => f(x._1, x._2))
  }

  /*
   * This combines map and group by, but also temporal
   */
  def aggregate[T: ClassTag, K: ClassTag, V: ClassTag](rel: RDD[(T, Interval)], groupBy: T => K, seqFunc: (T, V) => V, aggFunc: (V, V) => V, zeroVal: V): RDD[((K, Interval), V)] = {
    //tuples within the same group have to be broken up by intervals
    //to create matching groups
    val combOp = (a: List[Interval], b: List[Interval]) => {
      //a and b are sorted and each element is non-overlapping
      if (a.size == 0)
        b
      else if (b.size == 0)
        a
      else {
        //TODO: it may be more efficient to traverse each list
        //together left to right
        implicit val ord = TempGraphOps.dateOrdering
          (a.flatMap(x => Seq(x.start, x.end)) ++ b.flatMap(x => Seq(x.start, x.end)))
          .sortBy(x => x)
          .foldLeft(List[Interval](Interval(LocalDate.MIN, LocalDate.MIN)))({
            case (a,b) =>
              if (!a.head.end.equals(b))
                Interval(a.head.end, b) :: a
              else
                a
          }).reverse.tail.tail
      }
    }

    val intervals: RDD[(K, List[Interval])] = rel.map(x => (groupBy(x._1), x._2)).aggregateByKey(List[Interval]())(seqOp = (u: List[Interval], v: Interval) => combOp(u, List[Interval](v)), combOp)

    rel.map{ case (x, intv) => (groupBy(x), (intv, seqFunc(x, zeroVal)))}
      .join(intervals)
      .flatMap{ case (k,v) => v._2.filter(x => x.intersects(v._1._1)).map(x => ((k, x), v._1._2))}
      .reduceByKey(aggFunc)
      .map(x => ((x._1._1, x._1._2), x._2))

  }

}
