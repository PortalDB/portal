package edu.drexel.cs.dbgroup.graphxt.util

import edu.drexel.cs.dbgroup.graphxt._
import java.time.LocalDate

object GraphLoader {
  private var dataPath = ""
  private var graphType = "MG"

  //This is the general path, not for a specific dataset
  def setPath(path: String):Unit = dataPath = path
  def setGraphType(tp: String):Unit = graphType = tp

  def loadData(set: String, from: LocalDate, to: LocalDate):TemporalGraph[String,Int] = {
    //FIXME: make this not hard-coded but read from somewhere
    val path = set.toLowerCase() match {
      case "ngrams" => dataPath + "/nGrams"
      case "dblp" => dataPath + "/dblp"
      case "ukdelis" => dataPath + "/ukDelis"
    }
    graphType match {
      case "SG" =>
        SnapshotGraph.loadData(path, from, to)
      case "MG" =>
        MultiGraph.loadData(path, from, to)
      case "SGP" =>
        SnapshotGraphParallel.loadData(path, from, to)
      case "MGC" =>
        MultiGraphColumn.loadData(path, from, to)
      case "OG" =>
        OneGraph.loadData(path, from, to)
    }
  }
}
