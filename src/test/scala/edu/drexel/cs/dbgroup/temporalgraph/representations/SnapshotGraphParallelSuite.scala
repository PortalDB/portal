package edu.drexel.cs.dbgroup.temporalgraph.representations

import java.time.LocalDate
import edu.drexel.cs.dbgroup.temporalgraph.{Interval, ProgramContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite


class SnapshotGraphParallelSuite  extends FunSuite with BeforeAndAfter {

  before {
    if(ProgramContext.sc == null){
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME")).setMaster("local[2]")
      val sc = new SparkContext(conf)
      ProgramContext.setContext(sc)
    }
  }

  test("Testing select", representations){
    val graph1 = SnapshotGraphParallel.loadData("file:///C:/Users/shishir/temporaldata/dblp/", LocalDate.parse("2010-01-01"), LocalDate.parse("2012-01-01"))
    val graph2 = graph1.select(new Interval(LocalDate.parse("2010-01-01"), LocalDate.parse("2012-01-01")))
    val graph3 = graph1.select(new Interval(LocalDate.parse("2011-01-01"), LocalDate.parse("2012-01-01")))
    assert(graph1 === graph2)
    assert(graph1 != graph3)
    val size1 = "graph1 size " + graph1.size()
    val size3 = "graph3 size " + graph3.size()
    info(size1)
    info(size3)
  }
}
