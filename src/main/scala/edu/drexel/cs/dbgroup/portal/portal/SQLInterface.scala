package edu.drexel.cs.dbgroup.portal.portal

import java.sql.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{EdgeRDD, VertexId}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.types._

import edu.drexel.cs.dbgroup.portal._

object SQLInterface {

  /**
    * Add schema to vertexes RDD of TGraphWProperties and return a dataframe
    * @param temporalGraph The TGraphWProperties
    * @return The result dataframe after adding schema to vertexes RDD.
    */
  private final def convertGraphVertexToDataframe(temporalGraph: TGraphWProperties): DataFrame = {
    val vertexes: RDD[(VertexId, (Interval, VertexEdgeAttribute))] = temporalGraph.vertices
    val vertexSchema = temporalGraph.graphSpec.getVertexSchema()
    var schema =
      StructType(
        StructField("vid", LongType, false) ::
          StructField("start", DateType, false) ::
          StructField("end", DateType, false) ::
          Nil
      )


    //we cannot make it into a map because
    //in spark sql maps have to have known key and value types
    //and for us the value is a sequence of different types, depending on the property
    for (struct <- vertexSchema) {
      schema = schema.add(StructField(struct.name, struct.dataType, true))
    }

    val vertexRowRdd = vertexes.map(x => Row.fromSeq(Seq(x._1.toLong, Date.valueOf(x._2._1.start), Date.valueOf(x._2._1.end)) ++ convertAttribute(x._2._2, vertexSchema)))
    ProgramContext.getSession.createDataFrame(vertexRowRdd, schema)
  }

  /**
    * Add schema to edges RDD of TGraphWProperties and return a dataframe
    * @param temporalGraph The TGraphWProperties
    * @return The result dataframe after adding schema to edges RDD.
    */
  private final def convertGraphEdgeToDataframe(temporalGraph: TGraphWProperties): DataFrame = {
    val edges: RDD[TEdge[VertexEdgeAttribute]] = temporalGraph.edges
    val edgeSchema = temporalGraph.graphSpec.getEdgeSchema()
    var schema =
      StructType(
        StructField("eid", LongType, false) ::
          StructField("vid1", LongType, false) ::
          StructField("vid2", LongType, false) ::
          StructField("start", DateType, false) ::
          StructField("end", DateType, false) ::
          Nil
      )

    for (struct <- edgeSchema) {
      schema = schema.add(StructField(struct.name, ArrayType(struct.dataType), true))
    }

    val edgesRowRdd = edges.map(te => Row.fromSeq(Seq(te.eId, te.srcId, te.dstId, Date.valueOf(te.interval.start), Date.valueOf(te.interval.end)) ++ convertAttribute(te.attr, edgeSchema)))
    ProgramContext.getSession.createDataFrame(edgesRowRdd, schema)
  }

  /**
    * Run sql query on the vertexes of a TGraphWProperties.
    * @param query The sql query to run on the graph
    * @param temporalGraph The TGraphWProperties
    * @return The result dataframe after executing sql query on the graph
    */
  private final def runSQLQueryVertex(query: String, temporalGraph: TGraphWProperties): DataFrame = {
    //removing the function from the query
    val indexVertexFlat = query.indexOfSlice(".toVertices()")
    val sqlQuery = query.replace(".toVertices()", "")

    //find the table name
    val i = query.lastIndexOf(" ", indexVertexFlat)
    val tableName = query.substring(i + 1, indexVertexFlat)

    //creating temp table and executing the query
    val vertexDF = convertGraphVertexToDataframe(temporalGraph)
    vertexDF.createOrReplaceTempView(tableName)
    ProgramContext.getSession.sql(sqlQuery)
  }


  /**
    * Run sql query on the edges of the TGraphWProperties.
    * @param query The sql query to run on the graph
    * @param temporalGraph The TGraphWProperties
    * @return The result dataframe after executing sql query on the graph
    */
  private final def runSQLQueryEdge(query: String, temporalGraph: TGraphWProperties): DataFrame = {
    //removing the function from the query
    val indexEdgesFlat = query.indexOfSlice(".toEdges()")
    val sqlQuery = query.replace(".toEdges()", "")

    //finding the table name
    val i = query.lastIndexOf(" ", indexEdgesFlat)
    val tableName = query.substring(i + 1, indexEdgesFlat)

    //creating temp table and executing the query
    val edgeDF = convertGraphEdgeToDataframe(temporalGraph)
    edgeDF.createOrReplaceTempView(tableName)
    val output = ProgramContext.getSession.sql(sqlQuery)
    output.first()
    output
  }

  /**
    * Run sql query on a TGraphWProperties.
    * Run the query on either the Vertexes of the Graph or the Edges of the Graph
    * specified by the function passed in the query (.toVerticesFlat or .toEdgesFlat)
    * @param query The sql query to run on the graph
    * @param temporalGraph The TGraphWProperties
    * @return The result dataframe after executing sql query on the graph
    * @throws IllegalArgumentException if the query does not have either .toVerticesFlat() or .toEdgesFlat()
    */
  final def runSQLQuery(query: String, temporalGraph: TGraphWProperties): DataFrame = {
    val indexVertexFlat = query.indexOfSlice(".toVertices()")
    val indexEdgesFlat = query.indexOfSlice(".toEdges()")
    if (indexVertexFlat != -1) {
      val output = runSQLQueryVertex(query, temporalGraph)
      return output
    }
    else if (indexEdgesFlat != -1) {
      val output = runSQLQueryEdge(query, temporalGraph)
      return output
    }
    else {
      throw new IllegalArgumentException("Query does not contain .toVertices() or .toEdges()");
    }
  }

  private def convertAttribute(attr: VertexEdgeAttribute, schema: Seq[StructField]): Seq[Any] = {
    schema.map( f =>
      attr(f.name)
    )
  }
}
