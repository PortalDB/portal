package edu.drexel.cs.dbgroup.temporalgraph.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StructType,Metadata,StructField}
import org.apache.spark.sql.catalyst.expressions.{Attribute,AttributeReference}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.VertexId
import org.apache.spark.HashPartitioner

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.representations._
import java.time.LocalDate
import scala.util.matching.Regex

object GraphLoader {
  private var graphType = "SG"
  private var strategy = PartitionStrategyType.None
  private var runWidth = 8

  def setGraphType(tp: String):Unit = {
    tp match {
      case "SG" | "OG" | "HG" => graphType = tp
      case _ => throw new IllegalArgumentException("unknown graph type")
    }
  }
  def setStrategy(str: PartitionStrategyType.Value):Unit = strategy = str
  def setRunWidth(rw: Int):Unit = runWidth = rw

  //TODO: change to using reflection so that new data types can be added without recoding this
  //This is from the plain text file format with a single attribute
  def loadData(path: String, from: LocalDate, to: LocalDate):TGraphNoSchema[String,Int] = {
    //read files
    var minDate: LocalDate = from
    var maxDate: LocalDate = to
    var source: scala.io.Source = null
    var fs: FileSystem = null

    val pt: Path = new Path(path + "/Span.txt")
    val conf: Configuration = new Configuration()
    if (System.getenv("HADOOP_CONF_DIR") != "") {
      conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"))
    }
    fs = FileSystem.get(conf)
    source = scala.io.Source.fromInputStream(fs.open(pt))

    val lines = source.getLines
    val minin = LocalDate.parse(lines.next)
    val maxin = LocalDate.parse(lines.next)
    val res = Resolution.from(lines.next)

    if (minin.isAfter(from))
      minDate = minin
    if (maxin.isBefore(to))
      maxDate = maxin
    source.close()

    if (minDate.isAfter(maxDate) || minDate.isEqual(maxDate))
      throw new IllegalArgumentException("invalid date range")

    val end = res.minus(maxDate)
    val usersnp: RDD[(VertexId,(Interval,String))] = MultifileLoad.readNodes(path, minDate, end).flatMap{ x => 
      val (filename, line) = x
      val dt = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))
      val parts = line.split(",")
      val index = res.numBetween(minDate, dt)
      if (parts.size > 1 && parts.head != "" && index > -1) {
        Some(parts.head.toLong, (res.getInterval(dt), parts(1).toString))
      } else None
    }
    val users = TGraphNoSchema.coalesce(usersnp.partitionBy(new HashPartitioner(usersnp.partitions.size)))

    val linksnp: RDD[((VertexId,VertexId),(Interval,Int))] = MultifileLoad.readEdges(path, minDate, end)
      .flatMap{ x =>
      val (filename, line) = x
      val dt = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))
      if (!line.isEmpty && line(0) != '#') {
        val lineArray = line.split("\\s+")
        val srcId = lineArray(0).toLong
        val dstId = lineArray(1).toLong
        var attr = 0
        if(lineArray.length > 2){
          attr = lineArray(2).toInt
        }
        if (srcId > dstId)
          Some((dstId, srcId), (res.getInterval(dt),attr))
        else
          Some((srcId, dstId), (res.getInterval(dt),attr))
      } else None
    }
    val links = TGraphNoSchema.coalesce(linksnp.partitionBy(new HashPartitioner(linksnp.partitions.size)))

    graphType match {
      case "SG" =>
        SnapshotGraphParallel.fromRDDs(users, links, "Default", StorageLevel.MEMORY_ONLY_SER)
      case "OG" =>
        OneGraphColumn.fromRDDs(users, links, "Default", StorageLevel.MEMORY_ONLY_SER)
      case "HG" =>
        HybridGraph.fromRDDs(users, links, "Default", StorageLevel.MEMORY_ONLY_SER)
    }
  }

  def loadDataParquet(url: String): TGraphNoSchema[Array[Any], Array[Any]] = {
    val sqlContext = ProgramContext.getSqlContext
    import sqlContext.implicits._

    val users = sqlContext.read.parquet(url + "/nodes.parquet")
    val links = sqlContext.read.parquet(url + "/edges.parquet")

    val vs: RDD[(VertexId, (Interval, Array[Any]))] = users.map(row => (row.getLong(0), (Interval(row.getDate(1).toLocalDate(), row.getDate(2).toLocalDate()), row.toSeq.drop(3).toArray)))
    val es: RDD[((VertexId, VertexId), (Interval, Array[Any]))] = links.map(row => ((row.getLong(0), row.getLong(1)), (Interval(row.getDate(2).toLocalDate(), row.getDate(3).toLocalDate()), row.toSeq.drop(4).toArray)))

    graphType match {
      case "SG" =>
        SnapshotGraphParallel.fromRDDs(vs, es, Array[Any](), StorageLevel.MEMORY_ONLY_SER)
      case "OG" =>
        OneGraphColumn.fromRDDs(vs, es, Array[Any](), StorageLevel.MEMORY_ONLY_SER)
      case "HG" =>
        HybridGraph.fromRDDs(vs, es, Array[Any](), StorageLevel.MEMORY_ONLY_SER)
    }
  }

  def loadStructureOnlyParquet(url: String): TGraphNoSchema[Null, Null] = {
    val sqlContext = ProgramContext.getSqlContext
    import sqlContext.implicits._

    val users = sqlContext.read.parquet(url + "/nodes.parquet").select($"vid", $"estart", $"eend")
    val links = sqlContext.read.parquet(url + "/edges.parquet").select($"vid1", $"vid2", $"estart", $"eend")

    //map to rdds, coalesce
    val vs: RDD[(VertexId, (Interval, Null))] = users.map(row => (row.getLong(0), (Interval(row.getDate(1).toLocalDate(), row.getDate(2).toLocalDate()), null)))
    val es: RDD[((VertexId, VertexId), (Interval, Null))] = links.map(row => ((row.getLong(0), row.getLong(1)), (Interval(row.getDate(2).toLocalDate(), row.getDate(3).toLocalDate()), null)))

    graphType match {
      case "SG" =>
        SnapshotGraphParallel.fromRDDs(TGraphNoSchema.coalesce(vs), TGraphNoSchema.coalesce(es), null, StorageLevel.MEMORY_ONLY_SER)
      case "OG" =>
        OneGraphColumn.fromRDDs(TGraphNoSchema.coalesce(vs), TGraphNoSchema.coalesce(es), null, StorageLevel.MEMORY_ONLY_SER)
      case "HG" =>
        HybridGraph.fromRDDs(TGraphNoSchema.coalesce(vs), TGraphNoSchema.coalesce(es), null, StorageLevel.MEMORY_ONLY_SER)
    }

  }

  def loadDataWithSchema(url: String, schema: GraphSpec): TGraphWithSchema = {
    //TODO: make the data type not string

    val sqlContext = ProgramContext.getSqlContext
    val cov = schema.getVertexSchema.map(f => f.name)
    val coe = schema.getEdgeSchema.map(f => f.name)
    //TODO: we are ignoring the data types passed in the schema
    //and relying on the names being correct
    //add handling to match up fields and give them desired names and types
    val users = sqlContext.read.parquet(url + "/nodes.parquet").select(cov.head, cov.tail: _*)
    val links = sqlContext.read.parquet(url + "/edges.parquet").select(coe.head, coe.tail: _*)

    //if we are not loading all the columns, we need to coalesce
    val vs = if (schema.getVertexSchema.length == users.columns.length) users else TGraphWithSchema.coalesceV(users)
    val es = if (schema.getEdgeSchema.length == links.columns.length) links else TGraphWithSchema.coalesceE(links)

    graphType match {
      case "SG" =>
        SnapshotGraphWithSchema.fromDataFrames(vs, es, StorageLevel.MEMORY_ONLY_SER)
      case "OG" =>
        OneGraphWithSchema.fromDataFrames(vs, es, StorageLevel.MEMORY_ONLY_SER)
      case _ => throw new IllegalArgumentException("Unknown data structure type " + graphType)
    }
  }


  def loadGraphSpan(url: String): Interval = {
    var source: scala.io.Source = null
    var fs: FileSystem = null

    val pt: Path = new Path(url + "/Span.txt")
    val conf: Configuration = new Configuration()
    if (System.getenv("HADOOP_CONF_DIR") != "") {
      conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"))
    }
    fs = FileSystem.get(conf)
    source = scala.io.Source.fromInputStream(fs.open(pt))

    val lines = source.getLines
    val minin = LocalDate.parse(lines.next)
    val maxin = LocalDate.parse(lines.next)
    source.close()
    Interval(minin,maxin)
  }

/*
  def loadGraphDescription(url: String): GraphSpec = {
    //there should be a special file called graph.info
    //which contains the number of attributes and their name/type

    val pt: Path = new Path(url + "/graph.info")
    val conf: Configuration = new Configuration()    
    if (System.getenv("HADOOP_CONF_DIR") != "") {
      conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"))
    }
    val fs:FileSystem = FileSystem.get(conf)
    val source:scala.io.Source = scala.io.Source.fromInputStream(fs.open(pt))

    val lines = source.getLines
    val numVAttrs: Int = lines.next.toInt
    val vertexAttrs: Seq[StructField] = (0 until numVAttrs).map { index =>
      val nextAttr = lines.next.split(':')
      //the format is name:type
      StructField(nextAttr.head, TypeParser.parseType(nextAttr.last))
    }

    val numEAttrs: Int = lines.next.toInt
    val edgeAttrs: Seq[StructField] = (0 until numEAttrs).map { index =>
      val nextAttr = lines.next.split(':')
      //the format is name:type
      StructField(nextAttr.head, TypeParser.parseType(nextAttr.last))
    }

    source.close()          

    new GraphSpec(vertexAttrs, edgeAttrs)
  }
 */
}
