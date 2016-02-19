package edu.drexel.cs.dbgroup.graphxt.util

import edu.drexel.cs.dbgroup.graphxt._
import java.time.LocalDate

object GraphLoader {
  private var dataPath = ""
  private var graphType = "MG"
  private var strategy = PartitionStrategyType.None
  private var runWidth = 2

  //This is the general path, not for a specific dataset
  def setPath(path: String):Unit = dataPath = path
  def setGraphType(tp: String):Unit = graphType = tp
  def setStrategy(str: PartitionStrategyType.Value):Unit = strategy = str
  def setRunWidth(rw: Int):Unit = runWidth = rw

  def loadData(set: String, from: LocalDate, to: LocalDate):TemporalGraph[String,Int] = {
    //FIXME: make this not hard-coded but read from somewhere
    val path = set.toLowerCase() match {
      case "ngrams" => dataPath + "/nGrams"
      case "dblp" => dataPath + "/dblp"
      case "ukdelis" => dataPath + "/ukdelis"
    }
    graphType match {
      case "MG" =>
        MultiGraph.loadWithPartition(path, from, to, strategy, runWidth)
      case "SGP" =>
        SnapshotGraphParallel.loadWithPartition(path, from, to, strategy, runWidth)
      case "MGC" =>
        MultiGraphColumn.loadWithPartition(path, from, to, strategy, runWidth)
      case "OG" =>
        OneGraph.loadWithPartition(path, from, to, strategy, runWidth)
      case "OGC" =>
        OneGraphColumn.loadWithPartition(path, from, to, strategy, runWidth)
      case "HG" =>
        HybridGraph.loadWithPartition(path, from, to, strategy, runWidth)
    }
  }
}
