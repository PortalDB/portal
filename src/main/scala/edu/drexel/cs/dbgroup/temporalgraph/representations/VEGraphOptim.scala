package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.reflect.ClassTag
import org.apache.spark.rdd._
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.{TempGraphOps,TemporalAlgebra}

/**
  * VE with some methods implemented more efficiently
*/

class VEGraphOptim[VD: ClassTag, ED: ClassTag](verts: RDD[(VertexId, (Interval, VD))], edgs: RDD[TEdge[ED]], defValue: VD, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false) extends VEGraph[VD,ED](verts, edgs, defValue, storLevel, coal) {

  override def createAttributeNodes( vAggFunc: (VD, VD) => VD)(vgroupby: (VertexId, VD) => VertexId ): VEGraph[VD, ED] = {

    //FIXME? This approach requires each group's list to fit into memory
    val valIntervals: RDD[(VertexId, List[Interval])] = allVertices.map{ case (vid, (intv, attr)) => (vgroupby(vid, attr), intv)}.aggregateByKey(List[Interval]())(seqOp = (u: List[Interval], v: Interval) => TemporalAlgebra.combOp(u, List[Interval](v)), TemporalAlgebra.combOp)

    val newVerts: RDD[(VertexId, (Interval, VD))] = allVertices.map{ case (vid, (intv, attr)) => (vgroupby(vid, attr), (intv, attr))}
      .join(valIntervals)
      .flatMap{ case (k,v) => v._2.filter(x => x.intersects(v._1._1)).map(x => ((k,x), v._1._2))}
      .reduceByKey(vAggFunc)
      .map(x => (x._1._1, (x._1._2, x._2)))
  
    val newEdges: RDD[TEdge[ED]] = {
      val newVIds: RDD[(VertexId, (Interval, VertexId))] = allVertices.map{ case (vid, (intv, attr)) => (vid, (intv, vgroupby(vid, attr)))}
      //for each edge, similar except computing the new ids requires joins with V
      allEdges.map{e => (e.srcId, e)}
        .join(newVIds)
        .filter{ case (vid, (e, v)) => e.interval.intersects(v._1)}
        .map{ case (vid, (e, v)) => (e.dstId, (v._2, (Interval(TempGraphOps.maxDate(e.interval.start, v._1.start), TempGraphOps.minDate(e.interval.end, v._1.end)), (e.eId,e.attr))))}
        .join(newVIds)
        .filter{ case (vid, (e, v)) => e._2._1.intersects(v._1)}
        .map{ case (vid, (e, v)) => TEdge(e._2._2._1, e._1, v._2, Interval(TempGraphOps.maxDate(e._2._1.start, v._1.start), TempGraphOps.minDate(e._2._1.end, v._1.end)), e._2._2._2)}
    }

    fromRDDs(newVerts, newEdges, defaultValue, storageLevel, false)
  }

  override def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[TEdge[E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coal: Boolean = false): VEGraphOptim[V, E] = {
    VEGraphOptim.fromRDDs(verts, edgs, defVal, storLevel, coalesced = coal)
  }

  override protected def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): VEGraphOptim[V, E] = VEGraphOptim.emptyGraph(defVal)

}

object VEGraphOptim extends Serializable {
  def emptyGraph[V: ClassTag, E: ClassTag](defVal: V): VEGraphOptim[V, E] = new VEGraphOptim(ProgramContext.sc.emptyRDD, ProgramContext.sc.emptyRDD, defVal, coal = true)

  def fromRDDs[V: ClassTag, E: ClassTag](verts: RDD[(VertexId, (Interval, V))], edgs: RDD[TEdge[E]], defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): VEGraphOptim[V, E] = {
    val cverts = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(verts) else verts
    val cedges = if (ProgramContext.eagerCoalesce && !coalesced) TGraphNoSchema.coalesce(edgs.map(e => e.toPaired())).map(e => TEdge.apply(e._1,e._2)) else edgs
    val coal = coalesced | ProgramContext.eagerCoalesce

    new VEGraphOptim(cverts, cedges, defVal, storLevel, coal)
  }

  def fromDataFrames[V: ClassTag, E: ClassTag](verts: org.apache.spark.sql.DataFrame, edgs: org.apache.spark.sql.DataFrame, defVal: V, storLevel: StorageLevel = StorageLevel.MEMORY_ONLY, coalesced: Boolean = false): VEGraphOptim[V, E] = {
    val cverts: RDD[(VertexId, (Interval, V))] = verts.rdd.map(r => (r.getLong(0), (Interval(r.getLong(1), r.getLong(2)), r.getAs[V](3))))
    val ceds: RDD[TEdge[E]] = edgs.rdd.map(r => TEdge[E](r.getLong(0),r.getLong(1), r.getLong(2), Interval(r.getLong(3), r.getLong(4)), r.getAs[E](5)))
    fromRDDs(cverts, ceds, defVal, storLevel, coalesced)
  }

}
