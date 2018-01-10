package edu.drexel.cs.dbgroup.portal.tools

import java.io.FileWriter
import java.time.LocalDate

import edu.drexel.cs.dbgroup.portal.{Interval, TGraphNoSchema, ProgramContext}
import edu.drexel.cs.dbgroup.portal.util.GraphLoader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by shishir on 6/6/2016.
  */
object DegreeDistribution {
  //note: this does not remove ALL logging
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
  val sc = new SparkContext(conf)
  ProgramContext.setContext(sc)
  val sqlContext = new SQLContext(sc)


  def main(args: Array[String]): Unit ={
//    createDegreeDistribution("./dblp", "test.txt")
    //createDegreeDistribution("hdfs://master:9000/data/arxiv/", "DatasetStatistics/Degrees/Data/arxiv.txt")	
    //createDegreeDistribution("hdfs://master:9000/data/nGrams/", "DatasetStatistics/Degrees/Data/nGrams.txt")
    createDegreeDistribution("hdfs://master:9000/data/ukdelis/", "DatasetStatistics/Degrees/Data/ukdelis.txt")
  }

  def createDegreeDistribution(source:String, directoryName:String): Unit ={
    val fw = new FileWriter(directoryName, true)
    val HG = GraphLoader.buildHG(source, -1, -1, Interval(LocalDate.MIN, LocalDate.MAX))
    val degrees = HG.degree()


    //test data
//    val degrees = sc.parallelize((1L to 5L).map(x => (x, (Interval(LocalDate.parse("2015-01-01"), LocalDate.parse("2016-01-01")), Math.abs(x-4)))))

    //degree distribution by each year
    //val textFile = (1936 to 2015).map(x => degrees.filter(_._2._1.intersects(Interval(LocalDate.parse(x.toString + "-01-01")
    //, LocalDate.parse((x+1).toString + "-01-01")))).map(x => (x._2._2, 1)).reduceByKey(_+_).map(_.toString()).saveAsTextFile(x.toString))

    val degreeDistribution = degrees.map(x => (x._2._2, 1)).reduceByKey(_+_).sortByKey().collect()
    for ( x <- degreeDistribution ) {
      fw.write(x._1.toString() + " " + x._2.toString + "\n")
    }
    fw.close()
  }
}
