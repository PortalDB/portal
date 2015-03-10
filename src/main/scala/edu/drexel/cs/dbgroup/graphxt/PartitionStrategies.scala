package edu.drexel.cs.dbgroup.graphxt

import org.apache.spark.graphx.PartitionStrategy

class YearPartitionStrategy(yr: Int, mx: Int) extends PartitionStrategy {
  var year: Int = yr
  var maxYear: Int = mx

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    //FIXME: what if there are more partitions than years?
      year / (math.ceil((maxYear+1.0)/numParts)).toInt
  }
}
