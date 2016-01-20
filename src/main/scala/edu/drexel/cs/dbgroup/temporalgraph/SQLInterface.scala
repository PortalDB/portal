package edu.drexel.cs.dbgroup.temporalgraph
import _root_.edu.drexel.cs.dbgroup.temporalgraph.{Interval, TemporalGraphWithSchema}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeRDD, VertexId, VertexRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object SQLInterface{

  val sqlContext = new SQLContext(SparkContext.getOrCreate())

  final def convertGraphToDataframeVertex(temporalGraph: TemporalGraphWithSchema):DataFrame = {
    val vertexes: VertexRDD[(Interval, InternalRow)] = temporalGraph.verticesFlat
    val vertexSchema = temporalGraph.getSchema().getVertexSchema()
    var schema =
      StructType(
        StructField("ID", StringType, false) ::
          StructField("Start", StringType, false) ::
          StructField("Last", StringType, false) ::
          Nil
      )
    for (struct <- vertexSchema) {
      schema = schema.add(struct)
    }
    var internalRowSchema = vertexSchema.map(x=>x.dataType);
    val vertexRowRdd = vertexes.map(x=> Row.fromSeq(Seq(x._1.toString, x._2._1.start.toString,x._2._1.end.toString) ++ x._2._2.toSeq(internalRowSchema)))
    val vertexDF = sqlContext.createDataFrame(vertexRowRdd, schema)
    vertexDF
 	}

 	final def convertGraphToDataframeEdge(temporalGraph: TemporalGraphWithSchema):DataFrame   = {
    val edges: EdgeRDD[(Interval, InternalRow)] = temporalGraph.edgesFlat
    val edgeSchema = temporalGraph.getSchema().getEdgeSchema()
    var schema =
      StructType(
        StructField("srcId", StringType, false) ::
          StructField("dstId", StringType, false)::
          StructField("Start", StringType, false) ::
          StructField("Last", StringType, false) ::
          Nil
      )
    for (struct <- edgeSchema){
      schema = schema.add(struct)
    }
    var internalRowSchema = edgeSchema.map(x=>x.dataType);
    val edgesRowRdd = edges.map(x=> Row.fromSeq(Seq(x.srcId.toString, x.dstId.toString, x.attr._1.start.toString, x.attr._1.end.toString) ++ x.attr._2.toSeq(internalRowSchema)))
    val edgesDF = sqlContext.createDataFrame(edgesRowRdd, schema)
    edgesDF
 	}

 	final def runSQLQueryVertex (query: String, temporalGraph: TemporalGraphWithSchema) = {
    val vertexDF = convertGraphToDataframeVertex(temporalGraph)
    val tableName = "vertex"
    vertexDF.registerTempTable(tableName)
    val sqlQuery = query + tableName
    val output = sqlContext.sql(sqlQuery)
    output.show()
 	}

  final def runSQLQueryEdge(query: String, temporalGraph: TemporalGraphWithSchema) = {
    val edgeDF = convertGraphToDataframeEdge(temporalGraph)
    val tableName = "edge"
    edgeDF.registerTempTable(tableName)
    val sqlQuery = query + tableName
    val output = sqlContext.sql(sqlQuery)
    output.show()
  }
 }