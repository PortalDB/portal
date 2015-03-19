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
  def makeStrategy(tp: PartitionStrategyType.Value, index: Int, total: Int):PartitionStrategy = {
    tp match {
      case PartitionStrategyType.CanonicalRandomVertexCut => PartitionStrategy.CanonicalRandomVertexCut
      case PartitionStrategyType.EdgePartition2D => PartitionStrategy.EdgePartition2D
      case PartitionStrategyType.NaiveTemporal => new NaiveTemporalPartitionStrategy(index,total)
      case PartitionStrategyType.NaiveTemporalEdge => new NaiveTemporalEdgePartitioning(total)
      case PartitionStrategyType.ConsecutiveTemporal => new ConsecutiveTemporalPartitionStrategy(index, total)
      case PartitionStrategyType.ConsecutiveTemporalEdge => new ConsecutiveTemporalEdgePartitionStrategy(total)
      case PartitionStrategyType.HybridRandomTemporal => new HybridRandomCutPartitionStrategy(index)
      case PartitionStrategyType.HybridRandomEdgeTemporal => HybridRandomCutEdgePartitionStrategy
      case PartitionStrategyType.Hybrid2DTemporal => new Hybrid2DPartitionStrategy(index)
      case PartitionStrategyType.Hybrid2DEdgeTemporal => Hybrid2DEdgePartitionStrategy
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
class HybridRandomCutPartitionStrategy(in: Int) extends PartitionStrategy {
  val index: Int = in

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    if (src < dst) {
      math.abs((src, dst).hashCode()*index) % numParts
    } else {
      math.abs((dst, src).hashCode()*index) % numParts
    }
  }
}

//Hybrid (NTP + CRVS). Here, we have to decide ahead of time how many intervals a run should contain.
object HybridRandomCutEdgePartitionStrategy extends PartitionStrategyMoreInfo {

  //we only provide this here because for inheritance we have to.
  //it shouldn't be invoked
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    throw new SparkException("tried to invoke nonfunctional method getPartition")
  }

  override def getPartition[ED: ClassTag](e: Edge[ED], numParts: PartitionID): PartitionID = {
    val (atr,index:Int) = e.attr
    if (e.srcId < e.dstId) {
      math.abs((e.srcId, e.dstId).hashCode()*index) % numParts
    } else {
      math.abs((e.dstId, e.srcId).hashCode()*index) % numParts
    }
  }
}

class Hybrid2DPartitionStrategy(in: Int) extends PartitionStrategy {
  val index: Int = in

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    val ceilSqrtNumParts: PartitionID = math.ceil(math.sqrt(numParts)).toInt
    val col: PartitionID = (math.abs(src * index) % ceilSqrtNumParts).toInt
    val row: PartitionID = (math.abs(dst * index) % ceilSqrtNumParts).toInt
    (col * ceilSqrtNumParts + row) % numParts
  }
}

object Hybrid2DEdgePartitionStrategy extends PartitionStrategyMoreInfo {
  //we only provide this here because for inheritance we have to.
  //it shouldn't be invoked
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    throw new SparkException("tried to invoke nonfunctional method getPartition")
  }

  override def getPartition[ED: ClassTag](e: Edge[ED], numParts: PartitionID): PartitionID = {
    val (atr,index:Int) = e.attr
    val ceilSqrtNumParts: PartitionID = math.ceil(math.sqrt(numParts)).toInt
    val col: PartitionID = (math.abs(e.srcId * index) % ceilSqrtNumParts).toInt
    val row: PartitionID = (math.abs(e.dstId * index) % ceilSqrtNumParts).toInt
    (col * ceilSqrtNumParts + row) % numParts
  }
}
