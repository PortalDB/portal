package edu.drexel.cs.dbgroup.portal.util

import scala.collection.immutable.Map
import edu.drexel.cs.dbgroup.portal.{Interval,Resolution}

// linear trend estimate algorithm 
object LinearTrendEstimate {
  def calculateSlopeFromIntervals(intervals: Map[Interval, Double]) : Double = {
    //first find the common resolution
    val unit = intervals.keys.map{ intv => Resolution.between(intv.start, intv.end).unit }
      .reduce( (x,y) => if (x.compareTo(y) < 0) x else y)
    //val res = Resolution.between(LocalDate.MIN, unit.addTo(LocalDate.MIN))

    //get the smallest date as start
    val st = intervals.keys.map(ii => ii.start).reduce((x,y) => if (x.isBefore(y)) x else y)

    //now turn intervals into points by unit
    calculateSlope(intervals.flatMap{ case (k,v) =>
      val inst = unit.between(st, k.start).toInt
      val inen = unit.between(st, k.end).toInt
      (inst to inen).map(ii => (ii, v))
    })
  }

  /**
   * Run linear trend estimate on a given set of points
   *
   * @param the data points on which to compute LinearTrendEstimate
   * @return the slope of the trend line
   */
  def calculateSlope(points : Map[Int, Double]) : Double = {
      val numPoints = points.size;
      
      // compute average of x and y; xbar and ybar
      val sumx = points.foldLeft(0)((sum,kv) => sum + kv._1);
      val sumy = points.foldLeft(0.0)((sum,kv) => sum + kv._2);
      val xbar = sumx / numPoints;
      val ybar = sumy / numPoints;
      
      // compute slope and intercept stats
      val nxy = numPoints * xbar * ybar;
      val nxsqr = numPoints * sqr(xbar); 
      var sumxy = 0.0;
      var sumxsqr = 0;
      
      points.foreach{ point =>
        val (x, y) = point;
        val xdev = x - xbar;
        val ydev = y - ybar;
       
        sumxy += x * y;
        sumxsqr += sqr(x);
       
      };
      
      val beta1 = ((numPoints * sumxy) - (sumx * sumy)) / ((numPoints * sumxsqr) - sqr(sumx));
      val beta0 = (sumy - (beta1 * sumx)) / numPoints;
      
      //println("Resulting intercept: " + roundAt(beta0, 6));
      return roundAt(beta1, 6);
  };
  
  def sqr(x: Int) = x * x;
  
  def sqr(x: Double) = x * x;
  
  def roundAt(n: Double, p: Int): Double = { val s = math pow (10, p); (math round n * s) / s }
  
};
