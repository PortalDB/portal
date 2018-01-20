package edu.drexel.cs.dbgroup.portal.representations

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.SparkException
import org.apache.spark.graphx.impl.GraphXPartitionExtension._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphXPartitionExtension
import org.apache.spark.graphx.impl.PartitionStrategyMoreInfo

import edu.drexel.cs.dbgroup.portal._

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
    val (index: Int,atr) = e.attr
    if (totalIndices == numParts)
      index
    else if (totalIndices > numParts) //more snapshots than partitions
      index % numParts
    else { //more partitions than snapshots, need to split across partitions
      //compute over how many partitions to split
      var partitionsPerRun: Int = numParts / totalIndices
      val over: Int = numParts % totalIndices
      val addon: Int = if (index > over) over else index
      val partitionToRun = index * partitionsPerRun + addon
      if (index < over) 
        partitionsPerRun += 1
      if (partitionsPerRun > 1) { //split using CRVC
        if (e.srcId < e.dstId)
          math.abs((e.srcId, e.dstId).hashCode()) % partitionsPerRun + partitionToRun
        else
          math.abs((e.dstId, e.srcId).hashCode()) % partitionsPerRun + partitionToRun
      } else {
        partitionToRun
      }
    }
  }
}

//Naive temporal partitioning (NTP) for SG.  Since edges are not
//labelled with intervals (e.g., 1 = 1995 or 1 = [1995, 1999]), but whole
//graphs are, need to pass that information on construction.
//The Temporal order among
//intervals is ignored here, that is, neighboring time intervals are
//no more likely to be partitioned together than any two random
//intervals.
class NaiveTemporalPartitionStrategy(in: Int, ti: Int) extends PartitionStrategy {
  val index: Int = in
  val totalIndices: Int = ti

  //note: numParts here is for the whole TemporalGraph, not just one snapshot
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    if (totalIndices == numParts)
      index
    else if (totalIndices > numParts) //more snapshots than partitions
      index % numParts
    else { //more partitions than snapshots, need to split across partitions
      //compute over how many partitions to split
      var partitionsPerRun: Int = numParts / totalIndices
      val over: Int = numParts % totalIndices
      val addon: Int = if (index > over) over else index
      val partitionToRun = index * partitionsPerRun + addon
      if (index < over) 
        partitionsPerRun += 1
      if (partitionsPerRun > 1) { //split using CRVC
        if (src < dst)
          math.abs((src, dst).hashCode()) % partitionsPerRun + partitionToRun
        else
          math.abs((dst, src).hashCode()) % partitionsPerRun + partitionToRun
      } else {
        partitionToRun
      }
    }
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
    if (totalIndices == numParts)
      index
    else if (totalIndices > numParts) { //more snapshots than partitions
      val snapshotsPerRun: Int = totalIndices / numParts
      val over: Int = totalIndices % numParts
      val even: Int = math.ceil(totalIndices/numParts.toDouble).toInt * over
      if (index > even)
        (index - even) / snapshotsPerRun + over
      else
        index / math.ceil(totalIndices/numParts.toDouble).toInt
    } else { //more partitions than snapshots, need to split across partitions
      //compute over how many partitions to split
      var partitionsPerRun: Int = numParts / totalIndices
      val over: Int = numParts % totalIndices
      val addon: Int = if (index > over) over else index
      val partitionToRun = index * partitionsPerRun + addon
      if (index < over) 
        partitionsPerRun += 1
      if (partitionsPerRun > 1) { //split using CRVC
        if (src < dst)
          math.abs((src, dst).hashCode()) % partitionsPerRun + partitionToRun
        else
          math.abs((dst, src).hashCode()) % partitionsPerRun + partitionToRun
      } else {
        partitionToRun
      }
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
    val (index:Int,atr) = e.attr
    if (totalIndices == numParts)
      index
    else if (totalIndices > numParts) { //more snapshots than partitions
      val snapshotsPerRun: Int = totalIndices / numParts
      val over: Int = totalIndices % numParts
      val even: Int = math.ceil(totalIndices/numParts.toDouble).toInt * over
      if (index > even)
        (index - even) / snapshotsPerRun + over
      else
        index / math.ceil(totalIndices/numParts.toDouble).toInt
    } else { //more partitions than snapshots, need to split across partitions
      //compute over how many partitions to split
      var partitionsPerRun: Int = numParts / totalIndices
      val over: Int = numParts % totalIndices
      val addon: Int = if (index > over) over else index
      val partitionToRun = index * partitionsPerRun + addon
      if (index < over) 
        partitionsPerRun += 1
      //split using CRVC
      if (e.srcId < e.dstId)
        math.abs((e.srcId, e.dstId).hashCode()) % partitionsPerRun + partitionToRun
      else
        math.abs((e.dstId, e.srcId).hashCode()) % partitionsPerRun + partitionToRun
    }
  }
}

//Hybrid (NTP + CRVS). Here, we have to decide ahead of time how many intervals a run should contain.
class HybridRandomCutPartitionStrategy(in: Int, ti: Int, rs: Int) extends PartitionStrategy {
  val index: Int = in
  val totalSnapshots: Int = ti
  var runWidth: Int = rs

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    val numRuns: Int = math.ceil(totalSnapshots / runWidth.toDouble).toInt
    if (numRuns < numParts) { //more partitions or same number
      var partitionsPerRun: Int = numParts / numRuns
      val over: Int = numParts % numRuns
      val run: Int = index / runWidth
      val addon: Int = if (run > over) over else run
      val partitionToRun = run * partitionsPerRun + addon
      if (run < over)
        partitionsPerRun += 1

      if (src < dst)
        math.abs((src, dst).hashCode()) % partitionsPerRun + partitionToRun
      else
        math.abs((dst, src).hashCode()) % partitionsPerRun + partitionToRun
    } else {  //more runs than partitions
      val run: Int = index / runWidth
      val over: Int = numRuns % numParts
      val even: Int = math.ceil(numRuns/numParts.toDouble).toInt * over
      if (run > even)
        (run - even) / (numRuns / numParts) + over
      else
        run / math.ceil(numRuns/numParts.toDouble).toInt
    }
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
    val (index:Int,atr) = e.attr
    val numRuns: Int = math.ceil(totalSnapshots.toDouble / runWidth).toInt
    if (numRuns < numParts) { //more partitions or same number
      var partitionsPerRun: Int = numParts / numRuns
      val over: Int = numParts % numRuns
      val run: Int = index / runWidth
      val addon: Int = if (run > over) over else run
      val partitionToRun = run * partitionsPerRun + addon
      if (run < over)
        partitionsPerRun += 1

      if (e.srcId < e.dstId)
        math.abs((e.srcId, e.dstId).hashCode()) % partitionsPerRun + partitionToRun
      else
        math.abs((e.dstId, e.srcId).hashCode()) % partitionsPerRun + partitionToRun
    } else {  //more runs than partitions
      val run: Int = index / runWidth
      val over: Int = numRuns % numParts
      val even: Int = math.ceil(numRuns/numParts.toDouble).toInt * over
      if (run > even)
        (run - even) / (numRuns / numParts) + over
      else
        run / math.ceil(numRuns/numParts.toDouble).toInt
    }
  }
}

class Hybrid2DPartitionStrategy(in: Int, ti: Int, rs: Int) extends PartitionStrategy {
  val index: Int = in
  val totalSnapshots: Int = ti
  var runWidth: Int = rs

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    var numRuns : Int = math.ceil(totalSnapshots.toDouble / runWidth).toInt
    if (numRuns < numParts) { //more partitions or same number
      val mixingPrime: VertexId = 1125899906842597L
      var partitionsPerRun: Int = numParts / numRuns
      val over: Int = numParts % numRuns
      val run: Int = index / runWidth
      val addon: Int = if (run > over) over else run
      val partitionToRun = run * partitionsPerRun + addon
      if (run < over)
        partitionsPerRun += 1

      val ceilSqrtNumParts: Int = math.ceil(math.sqrt(partitionsPerRun)).toInt
      val col: PartitionID = (math.abs(src * mixingPrime) % ceilSqrtNumParts).toInt
      val row: PartitionID = (math.abs(dst * mixingPrime) % ceilSqrtNumParts).toInt

      val partitionWithinRun: Int = (col * ceilSqrtNumParts + row) % partitionsPerRun
      partitionToRun + partitionWithinRun

    } else {  //more runs than partitions
      val run: Int = index / runWidth
      val over: Int = numRuns % numParts
      val even: Int = math.ceil(numRuns/numParts.toDouble).toInt * over
      if (run > even)
        (run - even) / (numRuns / numParts) + over
      else
        run / math.ceil(numRuns/numParts.toDouble).toInt
    }
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
    val (index:Int,atr) = e.attr
    var numRuns : Int = math.ceil(totalSnapshots.toDouble / runWidth).toInt
    if (numRuns < numParts) { //more partitions or same number
      val mixingPrime: VertexId = 1125899906842597L
      var partitionsPerRun: Int = numParts / numRuns
      val over: Int = numParts % numRuns
      val run: Int = index / runWidth
      val addon: Int = if (run > over) over else run
      val partitionToRun = run * partitionsPerRun + addon
      if (run < over)
        partitionsPerRun += 1

      val ceilSqrtNumParts: Int = math.ceil(math.sqrt(partitionsPerRun)).toInt
      val col: PartitionID = (math.abs(e.srcId * mixingPrime) % ceilSqrtNumParts).toInt
      val row: PartitionID = (math.abs(e.dstId * mixingPrime) % ceilSqrtNumParts).toInt

      val partitionWithinRun: Int = (col * ceilSqrtNumParts + row) % partitionsPerRun
      partitionToRun + partitionWithinRun

    } else {  //more runs than partitions
      val run: Int = index / runWidth
      val over: Int = numRuns % numParts
      val even: Int = math.ceil(numRuns/numParts.toDouble).toInt * over
      if (run > even)
        (run - even) / (numRuns / numParts) + over
      else
        run / math.ceil(numRuns/numParts.toDouble).toInt
    }
  }
}
