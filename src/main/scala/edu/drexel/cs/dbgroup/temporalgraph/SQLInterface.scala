package edu.drexel.cs.dbgroup.temporalgraph

import java.sql.Date

import _root_.edu.drexel.cs.dbgroup.temporalgraph.{Interval, TemporalGraphWithSchema}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeRDD, VertexId, VertexRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.types._


object SQLInterface{

  val sqlContext = new SQLContext(SparkContext.getOrCreate())

  final def convertGraphToDataframeVertex(temporalGraph: TemporalGraphWithSchema):DataFrame = {
    val vertexes: VertexRDD[(Interval, InternalRow)] = temporalGraph.verticesFlat
    val vertexSchema = temporalGraph.getSchema().getVertexSchema()
    var schema =
      StructType(
        StructField("vid", LongType, false) ::
          StructField("Start", DateType, false) ::
          StructField("Last", DateType, false) ::
          Nil
      )
    for (struct <- vertexSchema) {
      schema = schema.add(struct)
    }
    var internalRowSchema = vertexSchema.map(x=>x.dataType);
    val vertexRowRdd = vertexes.map(x=> Row.fromSeq(Seq(x._1.toLong, Date.valueOf(x._2._1.start), Date.valueOf(x._2._1.end)) ++ x._2._2.toSeq(internalRowSchema)))
    val vertexDF = sqlContext.createDataFrame(vertexRowRdd, schema)
    vertexDF
 	}

 	final def convertGraphToDataframeEdge(temporalGraph: TemporalGraphWithSchema):DataFrame   = {
    val edges: EdgeRDD[(Interval, InternalRow)] = temporalGraph.edgesFlat
    val edgeSchema = temporalGraph.getSchema().getEdgeSchema()
    var schema =
      StructType(
        StructField("vid1", LongType, false) ::
          StructField("vid2", LongType, false)::
          StructField("Start", DateType, false) ::
          StructField("Last", DateType, false) ::
          Nil
      )
    for (struct <- edgeSchema){
      schema = schema.add(struct)
    }
    var internalRowSchema = edgeSchema.map(x=>x.dataType);
    val edgesRowRdd = edges.map(x=> Row.fromSeq(Seq(x.srcId.toLong, x.dstId.toLong, Date.valueOf(x.attr._1.start), Date.valueOf(x.attr._1.end)) ++ x.attr._2.toSeq(internalRowSchema)))
    sqlContext.createDataFrame(edgesRowRdd, schema)
 	}

 	final def runSQLQueryVertex (query: String, temporalGraph: TemporalGraphWithSchema):DataFrame = {
    //removing the function from the query
    val indexVertexFlat = query.indexOfSlice(".toVerticesFlat()")
    val sqlQuery = query.replace(".toVerticesFlat()", "")

    //find the table name
    val i = query.lastIndexOf(" ", indexVertexFlat)
    val tableName = query.substring(i+1, indexVertexFlat)

    val vertexDF = convertGraphToDataframeVertex(temporalGraph)
    vertexDF.registerTempTable(tableName)
    sqlContext.sql(sqlQuery)
 	}

  final def runSQLQueryEdge(query: String, temporalGraph: TemporalGraphWithSchema):DataFrame = {
    //removing the function from the query
    val indexEdgesFlat = query.indexOfSlice(".toEdgesFlat()")
    val sqlQuery = query.replace(".toEdgesFlat()", "")

    //finding the table name
    val i = query.lastIndexOf(" ", indexEdgesFlat)
    val tableName = query.substring(i+1, indexEdgesFlat)

//    var i = sqlQuery.indexOf("From")
//    var j = sqlQuery.indexOf(" ", i);
//    i = sqlQuery.indexOf(" ", j+1);
//    val tableName = sqlQuery.substring(j+1, i)

    val edgeDF = convertGraphToDataframeEdge(temporalGraph)
    edgeDF.registerTempTable(tableName)
    val output = sqlContext.sql(sqlQuery)
    output.first()
    output
  }

  final def runSQLQuery(query: String, temporalGraph: TemporalGraphWithSchema):DataFrame ={
    val indexVertexFlat = query.indexOfSlice(".toVerticesFlat()")
    val indexEdgesFlat = query.indexOfSlice(".toEdgesFlat()")
    if(indexVertexFlat != -1 ){
      val output = runSQLQueryVertex(query, temporalGraph)
      return output
    }
    if(indexEdgesFlat != -1 ) {
      val output = runSQLQueryEdge(query, temporalGraph)
      return output
    }
    null
  }
 }