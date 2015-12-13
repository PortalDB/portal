package edu.drexel.cs.dbgroup.temporalgraph.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StructType,Metadata,StructField}
import org.apache.spark.sql.catalyst.expressions.{Attribute,AttributeReference}
import org.apache.spark.sql.types._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.representations._
import java.time.LocalDate
import scala.util.matching.Regex

object GraphLoader {
  private var dataPath = ""
  private var graphType = "MG"
  private var strategy = PartitionStrategyType.None
  private var runWidth = 2

  //This is the general path, not for a specific dataset
  def setPath(path: String):Unit = dataPath = path
  def setGraphType(tp: String):Unit = graphType = tp
  def setStrategy(str: PartitionStrategyType.Value):Unit = strategy = str
  def setRunWidth(rw: Int):Unit = runWidth = rw

  //TODO: change to using reflection so that new data types can be added without recoding this
  def loadData(set: String, from: LocalDate, to: LocalDate):TemporalGraph[String,Int] = {
    //FIXME: make this not hard-coded but read from somewhere
    val path = set.toLowerCase() match {
      case "ngrams" => dataPath + "/nGrams"
      case "dblp" => dataPath + "/dblp"
      case "ukdelis" => dataPath + "/ukDelis"
    }
    graphType match {
      case "MG" =>
        MultiGraph.loadWithPartition(path, from, to, strategy, runWidth)
      case "SGP" =>
        SnapshotGraphParallel.loadWithPartition(path, from, to, strategy, runWidth)
      case "MGC" =>
        MultiGraphColumn.loadWithPartition(path, from, to, strategy, runWidth)
      case "OG" =>
        OneGraph.loadWithPartition(path, from, to, strategy, runWidth)
      case "OGC" =>
        OneGraphColumn.loadWithPartition(path, from, to, strategy, runWidth)
    }
  }

  def loadDataWithSchema(set: String, from: LocalDate, to: LocalDate, schema: StructType): TemporalGraphWithSchema = {
    //TODO!
    throw new UnsupportedOperationException("loadDataWithSchema not yet implemented")
  }

  def loadGraphSpan(url: String): (Interval, Resolution) = {
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
    val res = Resolution.from(lines.next)
    source.close()
    (Interval(minin,maxin),res)
  }

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
}
