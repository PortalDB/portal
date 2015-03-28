package edu.drexel.cs.dbgroup.graphxt

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.SparkException
import org.apache.spark.graphx.impl.GraphXPartitionExtension._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphXPartitionExtension
import org.apache.spark.graphx.impl.PartitionStrategyMoreInfo

object PartitionStrategyType extends Enumeration {
  val CanonicalRandomVertexCut, EdgePartition2D, NaiveTemporal, NaiveTemporalEdge, ConsecutiveTemporal, ConsecutiveTemporalEdge, HybridRandomTemporal, HybridRandomEdgeTemporal, Hybrid2DTemporal, Hybrid2DEdgeTemporal, None = Value
}

object PartitionStrategies {
  //a factory for different strategies
  def makeStrategy(tp: PartitionStrategyType.Value, index: Int, total: Int, runs: Int):PartitionStrategy = {
    tp match {
      case PartitionStrategyType.CanonicalRandomVertexCut => PartitionStrategy.CanonicalRandomVertexCut
      case PartitionStrategyType.EdgePartition2D => PartitionStrategy.EdgePartition2D
      case PartitionStrategyType.NaiveTemporal => new NaiveTemporalPartitionStrategy(index,total)
      case PartitionStrategyType.NaiveTemporalEdge => new NaiveTemporalEdgePartitioning(total)
      case PartitionStrategyType.ConsecutiveTemporal => new ConsecutiveTemporalPartitionStrategy(index, total)
      case PartitionStrategyType.ConsecutiveTemporalEdge => new ConsecutiveTemporalEdgePartitionStrategy(total)
      case PartitionStrategyType.HybridRandomTemporal => new HybridRandomCutPartitionStrategy(index,total,runs)
      case PartitionStrategyType.HybridRandomEdgeTemporal => new HybridRandomCutEdgePartitionStrategy(total,runs)
      case PartitionStrategyType.Hybrid2DTemporal => new Hybrid2DPartitionStrategy(index,total,runs)
      case PartitionStrategyType.Hybrid2DEdgeTemporal => new Hybrid2DEdgePartitionStrategy(total,runs)
    }
  }

}


//Naive temporal edge partitioning (NTP).  Let us assume that edges
//are labelled with intervals (e.g., 1 = 1995 or 1 = [1995, 1999]).
//This strategy assigns all edges that have the same interval label to
//the same partition.  If there are fewer partitions than intervals,
//then we first partition using NTP, and then partition further using
//CRVC or EP2D (we should try both).  In the Temporal order among
//intervals is ignored here, that is, neighboring time intervals are
//no more likely to be partitioned together than any two random
//intervals.
class NaiveTemporalEdgePartitioning(total: Int) extends PartitionStrategyMoreInfo {
  val totalIndices: Int = total

  //we only provide this here because for inheritance we have to.
  //it shouldn't be invoked
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    throw new SparkException("tried to invoke nonfunctional method getPartition")
  }

  def getPartition[ED: ClassTag](e:Edge[ED], numParts: PartitionID): PartitionID = {
    //assuming zero-based indexing for both indices and partitions
    //just return the index of the edge as long as there are enough partitions
    val (atr,ind:Int) = e.attr
    if (totalIndices <= numParts)
      ind
    else {
      //this is similar to the canonical random vertex cut but only uses the edge index
      //FIXME? change to straight up CRVC??
      ind % numParts
    }
  }
}

//Naive temporal partitioning (NTP) for SG.  Since edges are not
//labelled with intervals (e.g., 1 = 1995 or 1 = [1995, 1999]), but whole
//graphs are, need to pass that information on construction.
//This strategy effectively assigns all edges that have the same interval label to
//the same partition.  If there are fewer partitions than intervals,
//then we first partition using NTP, and then partition further using
//CRVC or EP2D (we should try both).  In the Temporal order among
//intervals is ignored here, that is, neighboring time intervals are
//no more likely to be partitioned together than any two random
//intervals.
class NaiveTemporalPartitionStrategy(in: Int, ti: Int) extends PartitionStrategy {
  val index: Int = in
  val totalIndices: Int = ti

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    //FIXME? change to straight up CRVC?
    index % numParts
  }
}

//Consecutive temporal partitioning (CTP) for SG.  The same as NTP for cases where 
//# partitions >= # intervals.  For the case when # partitions < # intervals, 
//we first split up interval labels into consecutive runs, e.g., 1,2,3 -> run 1; 
//4,5,6 -> run 2, and then send all edges corresponding to a run to a given partition. 
class ConsecutiveTemporalPartitionStrategy(in: Int, ti: Int) extends PartitionStrategy {
  val index: Int = in
  val totalIndices: Int = ti

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    if (numParts >= totalIndices)
      index
    else {
      index / (math.ceil((totalIndices)/numParts)).toInt
    }
  }
}

//Consecutive temporal edge partitioning (CTP).  The same as NTP for cases where 
//# partitions >= # intervals.  For the case when # partitions < # intervals, 
//we first split up interval labels into consecutive runs, e.g., 1,2,3 -> run 1; 
//4,5,6 -> run 2, and then send all edges corresponding to a run to a given partition. 
class ConsecutiveTemporalEdgePartitionStrategy(total: Int) extends PartitionStrategyMoreInfo {
  val totalIndices: Int = total

  //we only provide this here because for inheritance we have to.
  //it shouldn't be invoked
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    throw new SparkException("tried to invoke nonfunctional method getPartition")
  }

  override def getPartition[ED: ClassTag](e:Edge[ED], numParts: PartitionID): PartitionID = {
    val (atr,index:Int) = e.attr
    if (numParts >= totalIndices)
      index
    else {
      index / (math.ceil((totalIndices)/numParts)).toInt
    }
  }
}

//Hybrid (NTP + CRVS). Here, we have to decide ahead of time how many intervals a run should contain.
class HybridRandomCutPartitionStrategy(in: Int, ti: Int, rs: Int) extends PartitionStrategy {
  val snapshot: Int = in
  val totalSnapshots: Int = ti
  var runWidth: Int = rs

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    var numRuns: Int = (math.ceil(totalSnapshots / runWidth)).toInt
    if (numRuns > numParts) {
      numRuns = numParts
      runWidth  = math.ceil(totalSnapshots / numRuns).toInt
    }
    val partitionsPerRun:Int = (math.ceil(numParts / numRuns)).toInt
    val snapshotToRun:Int = (snapshot / runWidth)

    var	partitionWithinRun: Int	= 0
    if (src < dst) {
       partitionWithinRun = math.abs((src, dst).hashCode()) % partitionsPerRun
    } else {
       partitionWithinRun = math.abs((dst, src).hashCode()) % partitionsPerRun
    }
    snapshotToRun * partitionsPerRun + partitionWithinRun
  }
}

//Hybrid (NTP + CRVS). Here, we have to decide ahead of time how many intervals a run should contain.
class HybridRandomCutEdgePartitionStrategy(ti: Int, rs: Int) extends PartitionStrategyMoreInfo {
  val totalSnapshots: Int = ti
  var runWidth: Int = rs

  //we only provide this here because for inheritance we have to.
  //it shouldn't be invoked
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    throw new SparkException("tried to invoke nonfunctional method getPartition")
  }

  override def getPartition[ED: ClassTag](e: Edge[ED], numParts: PartitionID): PartitionID = {
    val (atr,index:Int) = e.attr
    var numRuns: Int = (math.ceil(totalSnapshots / runWidth)).toInt
    if (numRuns > numParts) {
      numRuns = numParts
      runWidth  = math.ceil(totalSnapshots / numRuns).toInt
    }
    val partitionsPerRun:Int = (math.ceil(numParts / numRuns)).toInt
    val snapshotToRun:Int = (index / runWidth)

    var	partitionWithinRun: Int	= 0
    if (e.srcId < e.dstId) {
       partitionWithinRun = math.abs((e.srcId, e.dstId).hashCode()) % partitionsPerRun
    } else {
       partitionWithinRun = math.abs((e.dstId, e.srcId).hashCode()) % partitionsPerRun
    }
    snapshotToRun * partitionsPerRun + partitionWithinRun
  }
}

class Hybrid2DPartitionStrategy(in: Int, ti: Int, rs: Int) extends PartitionStrategy {
  val snapshot: Int = in
  val totalSnapshots: Int = ti
  var runWidth: Int = rs

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    var numRuns : Int = (math.ceil(totalSnapshots / runWidth)).toInt
    if (numRuns	> numParts) {
      numRuns = numParts
      runWidth = math.ceil(totalSnapshots / numRuns).toInt
    }

    val	partitionsPerRun : Int = (math.ceil(numParts / numRuns)).toInt

    val	snapshotToRun: Int	= (snapshot / runWidth)
    val ceilSqrtNumParts: PartitionID = math.ceil(math.sqrt(partitionsPerRun)).toInt

    val col: PartitionID = (math.abs(src) % ceilSqrtNumParts).toInt
    val row: PartitionID = (math.abs(dst) % ceilSqrtNumParts).toInt
    val partitionWithinRun: Int = (col * ceilSqrtNumParts + row) % partitionsPerRun

    snapshotToRun * partitionsPerRun + partitionWithinRun
  }
}

class Hybrid2DEdgePartitionStrategy(ti: Int, rs: Int) extends PartitionStrategyMoreInfo {
  val totalSnapshots: Int = ti
  var runWidth: Int = rs

  //we only provide this here because for inheritance we have to.
  //it shouldn't be invoked
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    throw new SparkException("tried to invoke nonfunctional method getPartition")
  }

  override def getPartition[ED: ClassTag](e: Edge[ED], numParts: PartitionID): PartitionID = {
    val (atr,index:Int) = e.attr

    var numRuns: Int = (math.ceil(totalSnapshots / runWidth)).toInt
    if (numRuns	> numParts) {
      numRuns = numParts
      runWidth = math.ceil(totalSnapshots / numRuns).toInt
    }

    val	partitionsPerRun: Int = (math.ceil(numParts / numRuns)).toInt

    val	snapshotToRun: Int = (index / runWidth)
    val ceilSqrtNumParts: PartitionID = math.ceil(math.sqrt(partitionsPerRun)).toInt

    val col: PartitionID = (math.abs(e.srcId) % ceilSqrtNumParts).toInt
    val row: PartitionID = (math.abs(e.dstId) % ceilSqrtNumParts).toInt
    val partitionWithinRun: Int = (col * ceilSqrtNumParts + row) % partitionsPerRun

    snapshotToRun * partitionsPerRun + partitionWithinRun
  }
}
