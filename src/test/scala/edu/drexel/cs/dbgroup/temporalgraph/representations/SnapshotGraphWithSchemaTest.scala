package edu.drexel.cs.dbgroup.temporalgraph.representations

import org.scalatest.FunSuite
import java.time.LocalDate
import org.scalatest.Tag
import org.scalatest.BeforeAndAfter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import edu.drexel.cs.dbgroup.temporalgraph._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object representations extends Tag("edu.drexel.cs.dbgroup.temporalgraph.representations")

class SnapshotGraphWithSchemaSuite extends FunSuite with BeforeAndAfter {

	// before {

	// }

	// ignore("loading temporal graph", representations){
	// 	val graph = SnapshotGraphWithSchema.loadData("file:///C:/Users/shishir/temporaldata/dblp/", LocalDate.parse("2010-01-01"), LocalDate.parse("2012-01-01"))
	// 	assert(1 === 1)
	// }
}