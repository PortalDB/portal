package edu.drexel.cs.dbgroup.temporalgraph.representations

import scala.collection.parallel.ParSeq
import scala.collection.mutable.Buffer
import scala.reflect.ClassTag
import scala.util.control._
import java.time.LocalDate

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import org.apache.spark.SparkContext
import org.apache.spark.Partition

import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.storage.{RDDBlockId,StorageLevel}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow,NamedExpression,InterpretedProjection}
import org.apache.spark.sql.types.StructField

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.{MultifileLoad,GraphLoader,TypeParser}

class SnapshotGraphWithSchema(intvs: Seq[Interval], gps: ParSeq[Graph[InternalRow,InternalRow]], schema: GraphSpec) extends SnapshotGraphParallel[InternalRow,InternalRow](intvs, gps) with TemporalGraphWithSchema with Serializable {
  override def getSchema(): GraphSpec = schema

  //we can support filtering (i.e. keeping only certain columsn)
  //and aliasing
  override def projectVertices(input: Seq[NamedExpression]): TemporalGraphWithSchema = {
    //TODO: check whether this projection actually changes anything
    //before going through the effort
    val projection = new InterpretedProjection(input, schema.vertexSchemaAsAttributes)

    new SnapshotGraphWithSchema(intervals, graphs.map { g =>
      g.mapVertices( (id: VertexId, attr: InternalRow) => projection(attr))
    }, GraphSpec(input.map(_.toAttribute).map(a => StructField(a.name, a.dataType,a.nullable, a.metadata)), schema.getEdgeSchema))
  }

  override def projectEdges(input: Seq[NamedExpression]): TemporalGraphWithSchema = {
    //TODO: check whether this projection actually changes anything
    //before going through the effort
    val projection = new InterpretedProjection(input, schema.edgeSchemaAsAttributes)

    new SnapshotGraphWithSchema(intervals, graphs.map { g =>
      g.mapEdges{ e: Edge[InternalRow] => projection(e.attr)}
    }, GraphSpec(schema.getVertexSchema, input.map(_.toAttribute).map(a => StructField(a.name, a.dataType,a.nullable, a.metadata))))
  }
}

object SnapshotGraphWithSchema {
  final def loadData(dataPath: String, start: LocalDate, end: LocalDate): SnapshotGraphWithSchema = {
    loadWithSchema(dataPath, start, end, GraphLoader.loadGraphDescription(dataPath))
  }

  final def loadWithSchema(dataPath: String, start: LocalDate, end:LocalDate, schema:GraphSpec): SnapshotGraphWithSchema = {
    loadWithSchemaAndPartition(dataPath, start, end, schema, PartitionStrategyType.None, 0)
  }

  final def loadWithSchemaAndPartition(dataPath: String, start: LocalDate, end: LocalDate, schema: GraphSpec, strategy: PartitionStrategyType.Value, runWidth: Int): SnapshotGraphWithSchema = {
    var minDate: LocalDate = start
    var maxDate: LocalDate = end

    val (fullInterval: Interval, res: Resolution) = GraphLoader.loadGraphSpan(dataPath)
    if (fullInterval.start.isAfter(start)) 
      minDate = fullInterval.start
    if (fullInterval.end.isBefore(end)) 
      maxDate = fullInterval.end

    if (minDate.isAfter(maxDate) || minDate.isEqual(maxDate))
      throw new IllegalArgumentException("invalid date range")

    var intvs: Seq[Interval] = Seq[Interval]()
    var gps: ParSeq[Graph[InternalRow, InternalRow]] = ParSeq[Graph[InternalRow, InternalRow]]()
    var xx:LocalDate = minDate

    val conf: Configuration = new Configuration()
    if (System.getenv("HADOOP_CONF_DIR") != "") {
      conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"))
    }
    val fs: FileSystem = FileSystem.get(conf)

    val fullSchema = GraphLoader.loadGraphDescription(dataPath)
    //FIXME: throw exception if the schema passed in has fields
    //that the full schema does not have!

    val vertexAllFields = fullSchema.getVertexSchema
    val edgeAllFields = fullSchema.getEdgeSchema
    val (vertexFields: Array[Boolean], edgeFields: Array[Boolean]) = schema.getIndices(fullSchema)

    val total = res.numBetween(minDate, maxDate)
    while (xx.isBefore(maxDate)) {
      var nodesPath = dataPath + "/nodes/nodes" + xx.toString() + ".txt"
      var edgesPath = dataPath + "/edges/edges" + xx.toString() + ".txt"
      var numNodeParts = MultifileLoad.estimateParts(nodesPath) 
      var numEdgeParts = MultifileLoad.estimateParts(edgesPath) 
      
      //FIXME: comma is not a good separator
      val users: RDD[(VertexId, InternalRow)] = ProgramContext.sc.textFile(dataPath + "/nodes/nodes" + xx.toString() + ".txt", numNodeParts).map(line => line.split(",")).flatMap { parts =>
        if (parts.size >= 1 && parts.head != "") {
          val id: VertexId = parts.head.toLong

          //load based on the schema, which might skip some fields
          //but names have to be the same
          //id is not listed in the schema because it is required and assumed
          val row = vertexFields.zipWithIndex.map { case (field,index) =>
            if (field) TypeParser.parseValueByType(parts(index+1), vertexAllFields(index).dataType)
          }
          Some((id, new GenericInternalRow(row)))
        } else None
      }
      
      //FIXME: space is not a good delimiter
      val edgePath = dataPath + "/edges/edges" + xx.toString() + ".txt"
      val ept = new Path(edgePath)
      val edges: EdgeRDD[InternalRow] = if (fs.exists(ept) && fs.getFileStatus(ept).getLen > 0) {
        EdgeRDD.fromEdges(ProgramContext.sc.textFile(edgePath, numEdgeParts).map(line => line.split(" ")).flatMap { parts =>
          if (parts.size >= 2 && parts.head != "") {
            val vid1: VertexId = parts(0).toLong
            val vid2: VertexId = parts(0).toLong
            val row = edgeFields.zipWithIndex.map { case (field, index) =>
              if (field) TypeParser.parseValueByType(parts(index+2), edgeAllFields(index).dataType)
            }
            val e: Edge[InternalRow] = Edge(vid1, vid2, new GenericInternalRow(row))
            Some(e)
          } else None
        })
      } else EdgeRDD.fromEdges[InternalRow,InternalRow](ProgramContext.sc.emptyRDD)
      intvs = intvs :+ res.getInterval(xx)
      gps = if (strategy == PartitionStrategyType.None)
        gps :+ Graph(users, edges)
      else
        gps :+ Graph(users, edges).partitionBy(PartitionStrategies.makeStrategy(strategy, intvs.size + 1, total, runWidth), edges.partitions.size)
      xx = intvs.last.end
    }

    new SnapshotGraphWithSchema(intvs, gps, schema)
  }
}
