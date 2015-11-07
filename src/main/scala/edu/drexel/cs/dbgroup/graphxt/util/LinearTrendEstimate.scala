package edu.drexel.cs.dbgroup.graphxt.util

import scala.collection.immutable.Map

// linear trend estimate algorithm 
object LinearTrendEstimate {
  /**
   * Run linear trend estimate on a given set of points
   *
   * @param the data points on which to compute LinearTrendEstimate
   * @return the slope of the trend line
   */
  def calculateSlope(points : Map[Int, Double]) : Double = {
      val numPoints = points.size;
      
      // compute average of x and y; xbar and ybar
      val sumx = points.foldLeft(0.0)((sum,kv) => sum + kv._2);
      val sumy = points.foldLeft(0)((sum,kv) => sum + kv._1);
      val xbar = sumx / numPoints;
      val ybar = sumy / numPoints;
      
      // compute slope and intercept stats
      var xxbar = 0.0;
      var yybar = 0.0;
      var xybar = 0.0;
      
      points.foreach{ point =>
        val (y, x) = point;
        val xdev = x - xbar;
        val ydev = y - ybar;
       
        xxbar += sqr(xdev);
        yybar += sqr(ydev);
        xybar += xdev * ydev;
       
      };
      
      val beta1 = xybar / xxbar;
      val beta0 = (sumy - (beta1 * sumx)) / numPoints;
      
      //println("Resulting intercept: " + roundAt(beta0, ));
      return roundAt(beta1, 10);
  };
  
  def sqr(x: Int) = x * x;
  
  def sqr(x: Double) = x * x;
  
  def roundAt(n: Double, p: Int): Double = { val s = math pow (10, p); (math round n * s) / s }
  
};
