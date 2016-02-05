package edu.drexel.cs.dbgroup.temporalgraph

import java.sql.Date

import _root_.edu.drexel.cs.dbgroup.temporalgraph.{Interval, TemporalGraphWithSchema}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeRDD, VertexId, VertexRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.types._


object SQLInterface{

  private val sqlContext = new SQLContext(SparkContext.getOrCreate())

  /**
    * Add schema to vertexes RDD of TemporalGraphWithSchema and return a dataframe
    * @param temporalGraph The TemporalGraphWithSchema
    * @return The result dataframe after adding schema to vertexes RDD.
    */
  private final def convertGraphVertexToDataframe(temporalGraph: TemporalGraphWithSchema):DataFrame = {
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

  /**
    * Add schema to edges RDD of TemporalGraphWithSchema and return a dataframe
    * @param temporalGraph The TemporalGraphWithSchema
    * @return The result dataframe after adding schema to edges RDD.
    */
 	private final def convertGraphEdgeToDataframe(temporalGraph: TemporalGraphWithSchema):DataFrame   = {
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

  /**
    * Run sql query on the vertexes of a TemporalGraphWithSchema.
    * @param query The sql query to run on the graph
    * @param temporalGraph The TemporalGraphWithSchema
    * @return The result dataframe after executing sql query on the graph
    */
 	private final def runSQLQueryVertex (query: String, temporalGraph: TemporalGraphWithSchema):DataFrame = {
    //removing the function from the query
    val indexVertexFlat = query.indexOfSlice(".toVerticesFlat()")
    val sqlQuery = query.replace(".toVerticesFlat()", "")

    //find the table name
    val i = query.lastIndexOf(" ", indexVertexFlat)
    val tableName = query.substring(i+1, indexVertexFlat)

    //creating temp table and executing the query
    val vertexDF = convertGraphVertexToDataframe(temporalGraph)
    vertexDF.registerTempTable(tableName)
    sqlContext.sql(sqlQuery)
 	}


  /**
    * Run sql query on the edges of the TemporalGraphWithSchema.
    * @param query The sql query to run on the graph
    * @param temporalGraph The TemporalGraphWithSchema
    * @return The result dataframe after executing sql query on the graph
    */
  private final def runSQLQueryEdge(query: String, temporalGraph: TemporalGraphWithSchema):DataFrame = {
    //removing the function from the query
    val indexEdgesFlat = query.indexOfSlice(".toEdgesFlat()")
    val sqlQuery = query.replace(".toEdgesFlat()", "")

    //finding the table name
    val i = query.lastIndexOf(" ", indexEdgesFlat)
    val tableName = query.substring(i+1, indexEdgesFlat)

    //creating temp table and executing the query
    val edgeDF = convertGraphEdgeToDataframe(temporalGraph)
    edgeDF.registerTempTable(tableName)
    val output = sqlContext.sql(sqlQuery)
    output.first()
    output
  }

  /**
    * Run sql query on a TemporalGraphWithSchema.
    * Run the query on either the Vertexes of the Graph or the Edges of the Graph
    * specified by the function passed in the query (.toVerticesFlat or .toEdgesFlat)
    * @param query The sql query to run on the graph
    * @param temporalGraph The TemporalGraphWithSchema
    * @return The result dataframe after executing sql query on the graph
    * @throws IllegalArgumentException if the query does not have either .toVerticesFlat() or .toEdgesFlat()
    */
  final def runSQLQuery(query: String, temporalGraph: TemporalGraphWithSchema):DataFrame ={
    val indexVertexFlat = query.indexOfSlice(".toVerticesFlat()")
    val indexEdgesFlat = query.indexOfSlice(".toEdgesFlat()")
    if(indexVertexFlat != -1 ){
      val output = runSQLQueryVertex(query, temporalGraph)
      return output
    }
    else if(indexEdgesFlat != -1 ) {
      val output = runSQLQueryEdge(query, temporalGraph)
      return output
    }
    else{
      throw new IllegalArgumentException("Query does not contain .toVerticesFlat() or .toEdgesFlat()");
    }
  }
 }