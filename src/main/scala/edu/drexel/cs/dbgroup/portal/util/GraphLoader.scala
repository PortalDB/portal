package edu.drexel.cs.dbgroup.portal.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StructType,Metadata,StructField}
import org.apache.spark.sql.catalyst.expressions.{Attribute,AttributeReference}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset,Row,DataFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.VertexId
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import edu.drexel.cs.dbgroup.portal._
import edu.drexel.cs.dbgroup.portal.representations._
import java.time.LocalDate
import scala.util.matching.Regex
import scala.reflect._

object GraphLoader {

  //TODO: make a type for Node and type for Link(Edge) so that representations can
  //deal directly with Dataset[Node] and Dataset[Link] which will be
  //type-safer and easier to understand.
  //Issue: for the attribute, Node/Link has to be a template, but we don't know
  //its type until we load the schema from the dataset, by which point is too late

  def buildRG(url: String, vattrcol: Int, eattrcol: Int, bounds: Interval): RepresentativeGraph[Any, Any] = {
    //get the configuration option for snapshot groups
    val sg = System.getProperty("portal.partitions.sgroup", "")
    //make a filter. RG needs "spatial" layout, i.e. one sorted by time
    val filter = "_s_" + sg

    val (nodes, edges, intervals, deflt) = loadDataParquet(url, vattrcol, eattrcol, bounds, filter)
    //TODO: when loading only one part, having a partition group does not
    //make it by default uncoalesced
    //instead, move this logic into loadDataParquet method and see how many
    //are loaded first
    val col = sg match {
      case "" => true
      case _ => false
    }

    if (intervals.size > 0)
      RepresentativeGraph.fromDataFramesWithIndex[Any,Any](nodes, edges, intervals, deflt, StorageLevel.MEMORY_ONLY_SER, col)
    else
      RepresentativeGraph.fromDataFrames[Any,Any](nodes, edges, deflt, StorageLevel.MEMORY_ONLY_SER, col)
  }

  def buildOG(url: String, vattrcol: Int, eattrcol: Int, bounds: Interval): OneGraph[Any, Any] = {
    //get the configuration option for snapshot groups
    val sg = System.getProperty("portal.partitions.sgroup", "")
    //make a filter. OG needs "temporal" layout, i.e. one sorted by id
    val filter = "_t_" + sg

    val (nodes, edges, intervals, deflt) = loadDataParquet(url, vattrcol, eattrcol, bounds, filter)
    val col = sg match {
      case "" => true
      case _ => false
    }

    if (intervals.size > 0)
      OneGraph.fromDataFramesWithIndex[Any,Any](nodes, edges, intervals, deflt, StorageLevel.MEMORY_ONLY_SER, col)
    else
      OneGraph.fromDataFrames[Any,Any](nodes, edges, deflt, StorageLevel.MEMORY_ONLY_SER, col)

  }

  def buildOGC(url: String, vattrcol: Int, eattrcol: Int, bounds: Interval): OneGraphColumn[Any, Any] = {
    //get the configuration option for snapshot groups
    val sg = System.getProperty("portal.partitions.sgroup", "")
    //make a filter. OG needs "temporal" layout, i.e. one sorted by id
    val filter = "_t_" + sg

    val (nodes, edges, intervals, deflt) = loadDataParquet(url, vattrcol, eattrcol, bounds, filter)
    val col = sg match {
      case "" => true
      case _ => false
    }

    if (intervals.size > 0)
      OneGraphColumn.fromDataFramesWithIndex[Any,Any](nodes, edges, intervals, deflt, StorageLevel.MEMORY_ONLY_SER, col)
    else
      OneGraphColumn.fromDataFrames[Any,Any](nodes, edges, deflt, StorageLevel.MEMORY_ONLY_SER, col)

  }

//TODO: load interval index
  def buildHG(url: String, vattrcol: Int, eattrcol: Int, bounds: Interval): HybridGraph[Any, Any] = {
    //get the configuration option for snapshot groups
    val sg = System.getProperty("portal.partitions.sgroup", "")
    //want one graph in HG per SG group

    //make a filter. HG needs "temporal" layout, i.e. one sorted by id
    val filter = "_t_" + sg
    //separate dataframe for each path
    val nodePaths = getPaths(url, bounds, "nodes" + filter)
    val nh = ProgramContext.getSession.read.parquet(nodePaths.head)
    //val nodeschema = nh.schema
    val nreader = ProgramContext.getSession.read.schema(nh.schema)
    var nodeDFs = nh +: nodePaths.tail.map(nf => nreader.parquet(nf))
    val edgePaths = getPaths(url, bounds, "edges" + filter)
    val eh = ProgramContext.getSession.read.parquet(edgePaths.head)
    //val edgeschema = eh.schema
    val ereader = ProgramContext.getSession.read.schema(eh.schema)
    var edgeDFs = eh +: edgePaths.tail.map(nf => ereader.parquet(nf))

    //select within bounds
    if (bounds.start != LocalDate.MIN || bounds.end != LocalDate.MAX) {
       val secs1 = math.floor(DateTimeUtils.daysToMillis(bounds.start.toEpochDay().toInt).toDouble / 1000L).toLong
       val secs2 = math.floor(DateTimeUtils.daysToMillis(bounds.end.toEpochDay().toInt).toDouble / 1000L).toLong
       nodeDFs = nodeDFs.zip(nodePaths).map{ case (nf,dp) => 
         if (bounds.contains(Interval.parse(dp.takeRight(21)))) nf else 
            nf.filter("NOT (estart >= " + secs2 + " OR eend <= " + secs1 + ")").withColumn("estart", greatest(nf("estart"), lit(secs1))).withColumn("eend", least(nf("eend"), lit(secs2)))
       }
       edgeDFs = edgeDFs.zip(edgePaths).map{ case (nf,dp) => 
          if (bounds.contains(Interval.parse(dp.takeRight(21)))) nf else
            nf.filter("NOT (estart >= " + secs2 + " OR eend <= " + secs1 + ")").withColumn("estart", greatest(nf("estart"), lit(secs1))).withColumn("eend", least(nf("eend"), lit(secs2)))
       }
    } 

    //the schema should be the same in each df
    val vattr = 2 + vattrcol
    if (nodeDFs.head.schema.fields.size <= vattr)
      throw new IllegalArgumentException("requested column index " + vattrcol + " which does not exist in the data")
    val eattr = 4 + eattrcol
    var ec = eattrcol
    if (edgeDFs.head.schema.fields.size <= eattr)
      ec = -1
      //throw new IllegalArgumentException("requested column index " + eattrcol + " which does not exist in the data")

    //if there are more fields in the schema, add the select statement
    if (vattrcol == -1) {
      if (nodeDFs.head.schema.fields.size > 3)
        nodeDFs = nodeDFs.map(nf => nf.select("vid", "estart", "eend"))
      nodeDFs = nodeDFs.map(nf => nf.withColumn("attr", lit(true)))
    }
    else if (nodeDFs.head.schema.fields.size > 4)
      nodeDFs = nodeDFs.map(nf => nf.select("vid", "estart", "eend", nf.schema.fields(vattr).name))
    if (ec == -1) {
      if (edgeDFs.head.schema.fields.size > 5)
        edgeDFs = edgeDFs.map(nf => nf.select("eid", "vid1", "vid2", "estart", "eend"))
      edgeDFs = edgeDFs.map(nf => nf.withColumn("attr", lit(true)))
    }
    else if (edgeDFs.head.schema.fields.size > 6)
      edgeDFs = edgeDFs.map(nf => nf.select("eid", "vid1", "vid2", "estart", "eend", nf.schema.fields(eattr).name))
    
    val col: Boolean = nodeDFs.size < 2
    val deflt: Any = if (vattrcol == -1) false else nodeDFs.head.schema.fields(vattr).dataType match {
      case StringType => ""
      case IntegerType => -1
      case LongType => -1L
      case DoubleType => -1.0
      case _ => null
    }
    HybridGraph.fromDataFrames[Any,Any](nodeDFs, edgeDFs, deflt, StorageLevel.MEMORY_ONLY_SER, col)

  }

  def buildVE(url: String, vattrcol: Int, eattrcol: Int, bounds: Interval): VEGraph[Any, Any] = {
    //get the configuration option for snapshot groups
    val sg = System.getProperty("portal.partitions.sgroup", "")
    val woptim: Boolean = System.getProperty("portal.vegraph.optim", "false").toBoolean
    //make a filter. VE needs "temporal" layout, i.e. one sorted by id
    val filter = "_t_" + sg

    val (nodes, edges, intervals, deflt) = loadDataParquet(url, vattrcol, eattrcol, bounds, filter)
    val col = sg match {
      case "" => true
      case _ => false
    }

    if (intervals.size > 0) {
      if (woptim)
        VEGraphOptim.fromDataFramesWithIndex[Any,Any](nodes, edges, intervals, deflt, StorageLevel.MEMORY_ONLY_SER, col)
      else
        VEGraph.fromDataFramesWithIndex[Any,Any](nodes, edges, intervals, deflt, StorageLevel.MEMORY_ONLY_SER, col)
    } else {
      if (woptim)
        VEGraphOptim.fromDataFrames[Any,Any](nodes, edges, deflt, StorageLevel.MEMORY_ONLY_SER, col)
      else
        VEGraph.fromDataFrames[Any,Any](nodes, edges, deflt, StorageLevel.MEMORY_ONLY_SER, col)
    }
  }

  private def loadDataParquet(url: String, vattrcol: Int, eattrcol: Int, bounds: Interval, filter: String): (DataFrame, DataFrame, Array[Interval], Any) = {
    val nodesFiles = getPaths(url, bounds, "nodes" + filter)
    val edgesFiles = getPaths(url, bounds, "edges" + filter)
    val intervalFiles = url + "/intervals"

    var users = ProgramContext.getSession.read.parquet(nodesFiles:_*)
    var links = ProgramContext.getSession.read.parquet(edgesFiles:_*)

    //load the intervals if index option on
    val withIndex = System.getProperty("portal.index", "false").toBoolean
    var intervals = if (withIndex) {
      ProgramContext.getSession.read.parquet(intervalFiles).rdd.map(r => r.getLong(0)).collect.sliding(2).map(lst => Interval(lst(0), lst(1))).toArray
    } else Array[Interval]()

    //select within bounds
    if (bounds.start != LocalDate.MIN || bounds.end != LocalDate.MAX) {
      val secs1 = math.floor(DateTimeUtils.daysToMillis(bounds.start.toEpochDay().toInt).toDouble / 1000L).toLong
      val secs2 = math.floor(DateTimeUtils.daysToMillis(bounds.end.toEpochDay().toInt).toDouble / 1000L).toLong
      users = users.filter("NOT (estart >= " + secs2 + " OR eend <= " + secs1 + ")").withColumn("estart", greatest(users("estart"), lit(secs1))).withColumn("eend", least(users("eend"), lit(secs2)))
      links = links.filter("NOT (estart >= " + secs2 + " OR eend <= " + secs1 + ")").withColumn("estart", greatest(links("estart"), lit(secs1))).withColumn("eend", least(links("eend"), lit(secs2)))

      if (intervals.size > 0) {
        val startBound = if (bounds.start.isAfter(intervals.head.start)) bounds.start else intervals.head.start
        val endBound = if (bounds.end.isBefore(intervals.last.end)) bounds.end else intervals.last.end
        val selectBound:Interval = Interval(startBound, endBound)
        val selectStart:Int = intervals.indexWhere(ii => ii.intersects(selectBound))
        val selectStop:Int = intervals.lastIndexWhere(ii => ii.intersects(selectBound))
        val zipped = intervals.zipWithIndex.filter(intv => intv._1.intersects(selectBound))
        intervals = intervals.slice(selectStart, selectStop+1).map(intv => if (intv.start.isBefore(startBound) || intv.end.isAfter(endBound)) intv.intersection(selectBound).get else intv)
      }
    }

    val vattr = 2 + vattrcol
    if (users.schema.fields.size <= vattr)
      throw new IllegalArgumentException("requested column index " + vattrcol + " which does not exist in the data")
    var ec = eattrcol
    val eattr = 4 + eattrcol
    if (links.schema.fields.size <= eattr)
      ec = -1
      //throw new IllegalArgumentException("requested column index " + eattrcol + " which does not exist in the data")

    val deflt: Any = if (vattrcol == -1) false else users.schema.fields(vattr).dataType match {
      case StringType => ""
      case IntegerType => -1
      case LongType => -1L
      case DoubleType => -1.0
      case _ => null
    }

    //if there are more fields in the schema, add the select statement
    //FIXME: if there are multiple fields and we only load one
    //then the data may be uncoalesced
    if (vattrcol == -1) {
      if (users.schema.fields.size > 3)
        users = users.select("vid", "estart", "eend")
      users = users.withColumn("attr", lit(true))
    }
    else if (users.schema.fields.size > 4)
      users = users.select("vid", "estart", "eend", users.schema.fields(vattr).name)
    if (ec == -1) {
      if (links.schema.fields.size > 5)
        links = links.select("eid", "vid1", "vid2", "estart", "eend")
      links = links.withColumn("attr", lit(true))
    }
    else if (links.schema.fields.size > 6)
      links = links.select("eid", "vid1", "vid2", "estart", "eend", links.schema.fields(eattr).name)

    (users, links, intervals, deflt)
  }

  def loadDataPropertyModel(url: String): TGraphWProperties = {
    val users = ProgramContext.getSession.read.parquet(url + "/nodes.parquet")
    val links = ProgramContext.getSession.read.parquet(url + "/edges.parquet")

    //load each column as a property with that key
    //TODO when we have the concrete property bag implementation
    //for now, empty graph
    null
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

  def loadGraphDescription(url: String): GraphSpec = {
    //there should be a special file called graph.info
    //which contains the number of attributes and their name/type
    //TODO: this method should use the schema in the parquet file
    //instead of a special file

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

  /* 
   * Return all the directories within the source that contain 
   * snapshot groups intersecting with the interval in question
   * Assumes that the directory has the snapshot groups directly in it
   * and that each snapshot group is named with the interval it contains.
   */
  def getPaths(path: String, intv: Interval, filter: String): Array[String] = {
    //get a listing of directories from path
    val pt: Path = new Path(path)
    val conf: Configuration = new Configuration()
    if (System.getenv("HADOOP_CONF_DIR") != "") {
      conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"))
    }
    val filterP = if (filter.endsWith("_")) filter else filter + "_"
    val pathFilter = new PathFilter {
      def accept(p: Path): Boolean = {
        val pat = (filterP+"""\d""").r
        pat.findFirstIn(p.getName()).isDefined
      }
    }
    val status = FileSystem.get(conf).listStatus(pt, pathFilter)
    status.map(x => x.getPath()).filter(x => Interval.parse(x.getName().takeRight(21)).intersects(intv)).map(x => x.toString())
  }

  def getParquet(paths: Array[String], point: LocalDate): DataFrame = {
    val file = paths.filter(x => Interval.parse(x.takeRight(21)).contains(point))
    if (file.size > 0) {
      ProgramContext.getSession.read.parquet(file.head)
    } else { 
      ProgramContext.getSession.emptyDataFrame
    }
  }

  def getParquet(paths: Array[String], intv: Interval): DataFrame = {
    val file = paths.filter(x => Interval.parse(x.takeRight(21)).intersects(intv))
    if (file.size > 0) {
      ProgramContext.getSession.read.parquet(file:_*)
    } else ProgramContext.getSession.emptyDataFrame
  }

}
