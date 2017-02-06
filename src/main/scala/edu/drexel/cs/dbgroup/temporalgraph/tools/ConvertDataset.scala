package edu.drexel.cs.dbgroup.temporalgraph.tools

import java.time.Period
import java.time.LocalDate
import java.sql.Date
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.fs._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row,DataFrame}
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType,TimestampType}
import org.apache.spark.util.SizeEstimator

import edu.drexel.cs.dbgroup.temporalgraph._
import edu.drexel.cs.dbgroup.temporalgraph.util.{TempGraphOps,GraphSplitter}

/**
  * Takes parquet datasets and produces either structual
  * or temporal locality dataset
  * optionally splits into multiple groups
  * Note: Spark supports DataFrame bucketing. However, there is no user
  * control for how to compute buckets - it is essentially hash partitioning
  * by specified values.  Spark also supports DataFrame partitioning during a write
  * but with the exact same constraints.  Thus we use neither here as we want
  * tighter control over what goes in which bucket/group.
  * 
*/
object ConvertDataset {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("DataNucleus").setLevel(Level.OFF)
    // settings to pass are master, jars and other configurations
    var conf = new SparkConf().setAppName("Dataset Converter").setSparkHome(System.getenv("SPARK_HOME"))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.network.timeout", "240")   

    //directory where we assume nodes.parquet and edges.parquet live
    var source: String = ""
    var locality = Locality.Temporal
    var split = SnapshotGroup.None
    //these are just some default values, we get the real ones from command line
    var widthTime = Resolution.zero()
    var widthRGs = 8
    var buckets = 16
    var ratioT = 0.5
    var dest: String = ""

    for (i <- 0 until args.length) {
      args(i) match {
        case "--type" =>
          locality = Locality.withName(args(i+1))
        case "--split" =>
          split = SnapshotGroup.withName(args(i+1))
          split match {
            case SnapshotGroup.WidthTime => widthTime = Resolution.from(args(i+2))
            case SnapshotGroup.WidthRGs => widthRGs = args(i+2).toInt
            case SnapshotGroup.Depth => buckets = args(i+2).toInt
            case SnapshotGroup.Redundancy => ratioT = args(i+2).toInt / 100.0 //percentage, 0-100, converted to 0-1 ratio
          }
        case "--data" =>
          source = args(i+1)
        case "--path" =>
          dest = args(i+1)
        case _ =>
      }
    }

    if (source == "") {
      println("need source dataset to convert, exiting")
      return
    }

    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)
    val sqlContext = ProgramContext.getSession
    //import sqlContext.implicits._
    sqlContext.conf.set("spark.sql.files.maxPartitionBytes", "16777216")
    sqlContext.conf.set("spark.sql.parquet.compression.codec", "gzip")

    val cd = new ConvertDataset(source, locality, split, widthTime, widthRGs, buckets, ratioT, dest)
    cd.convert()

    sc.stop
  }
}

class ConvertDataset(source: String, locality: Locality.Value, split: SnapshotGroup.Value, widthTime: Resolution, widthRGs: Int, buckets: Int, ratioT: Double, dest: String) {
  final val blocksize = 1024 * 1024 * 512.0

  def convert(): Unit = {
    //load the datasets
    val nodes = ProgramContext.getSession.read.parquet(source + "/nodes.parquet")
    val edges = ProgramContext.getSession.read.parquet(source + "/edges.parquet")
    val (minDate, maxDate) = { val tmp = nodes.agg(min("estart"), max("eend")).collect().head; (tmp.getDate(0).toLocalDate(), tmp.getDate(1).toLocalDate()) }

    val intervals: Seq[Interval] = split match {
      case SnapshotGroup.None => Seq(Interval(minDate, maxDate))
      case SnapshotGroup.WidthTime => Interval(minDate, maxDate).split(widthTime, minDate).map(_._1)
      case SnapshotGroup.WidthRGs => 
        implicit val ord = TempGraphOps.dateOrdering
        nodes.select("estart").distinct().rdd.map(r => r.getDate(0).toLocalDate()).union(nodes.select("eend").distinct().rdd.map(r => r.getDate(0).toLocalDate())).union(edges.select("estart").distinct().rdd.map(r => r.getDate(0).toLocalDate())).union(edges.select("eend").distinct().rdd.map(r => r.getDate(0).toLocalDate())).distinct().sortBy(c => c, true, 1).sliding(widthRGs+1, widthRGs).map(lst => Interval(lst(0), lst.last)).collect().toSeq
      case SnapshotGroup.Depth =>
        val splitter = new GraphSplitter(nodes.select("estart", "eend").rdd.map(r => Interval(r.getDate(0), r.getDate(1))).union(edges.select("estart", "eend").rdd.map(r => Interval(r.getDate(0), r.getDate(1)))))
        val (c, splitters) = splitter.findSplit(buckets-1)
        if (c < 1) {
          println("could not find a valid optimal split with " + buckets + " buckets, exiting")
          return
        }
        (minDate +: splitters :+ maxDate).sliding(2).map(lst => Interval(lst(0), lst(1))).toSeq
      case SnapshotGroup.Redundancy => //controlled by threashold of similarity
        implicit val ord = TempGraphOps.dateOrdering
        val rgs = nodes.select("estart").distinct().rdd.map(r => r.getDate(0).toLocalDate()).union(nodes.select("eend").distinct().rdd.map(r => r.getDate(0).toLocalDate())).union(edges.select("estart").distinct().rdd.map(r => r.getDate(0).toLocalDate())).union(edges.select("eend").distinct().rdd.map(r => r.getDate(0).toLocalDate())).distinct().sortBy(c => c, true, 1).sliding(2).map(lst => Interval(lst(0), lst.last)).collect().toSeq
        //we compute intervals such that the first snapshot in each group
        //is no more than threshold percent similar to the previous one
        var buffer = ArrayBuffer.empty[Interval]
        var rgCount:Int = 1
        var headnodes = nodes.filter("estart < '" + rgs.head.end + "'")
        var headedges = edges.filter("estart < '" + rgs.head.end + "'")
        var nodescnt = headnodes.count
        var edgescnt = headedges.count
        for (i <- 1 until rgs.size) {
          //use GES: 2* size of diff / sum of sizes
          val nxtnodes = nodes.filter("NOT (estart >= '" + rgs(i).end + "' OR eend <= '" + rgs(i).start + "')")
          val nxtedgs = edges.filter("NOT (estart >= '" + rgs(i).end + "' OR eend <= '" + rgs(i).start + "')")
          val common = nxtnodes.intersect(headnodes).count + nxtedgs.intersect(headedges).count
          val nxtncnt = nxtnodes.count
          val nxtecnt = nxtedgs.count
          //commonality ratio
          val ratio = 2.0 * common / (nodescnt + edgescnt + nxtncnt + nxtecnt)
          println("commonality ratio to " + rgs(i) + ": " + ratio)
          //TODO: replace rgcount > 2 with a better heuristic
          if (ratio < ratioT && rgCount > 2) {
            rgCount = 1
            println("not very common, cutting group")
            buffer = if (buffer.size > 0) buffer :+ Interval(buffer.last.end, rgs(i).start) else buffer :+ Interval(rgs.head.start, rgs(i).start)
            headnodes = nxtnodes
            headedges = nxtedgs
            nodescnt = nxtncnt
            edgescnt = nxtecnt
          } else rgCount += 1
        }
        //add the last one
        buffer += Interval(buffer.last.end, rgs.last.end)
        buffer.toSeq
    }

    convert(nodes, intervals, minDate, dest + "/nodes")
    convert(edges, intervals, minDate, dest + "/edges")
  }

  private def convert(data: DataFrame, intervals: Seq[Interval], minDate: LocalDate, dest: String): Unit = {
    //get the name of the id column
    val idname = data.columns.filter(x => x.startsWith("vid")).head
    //get the index of estart column
    val startIndex = data.columns.indexOf("estart")

    println("splitting into intervals:\n" + intervals.mkString("\n"))

    val lst = locality match {
      case Locality.Temporal => "_t"
      case Locality.Structural => "_s"
    }

    val pname = split match {
      case SnapshotGroup.None => lst + "_"
      case SnapshotGroup.WidthTime => lst + "_wt_"
      case SnapshotGroup.WidthRGs => lst + "_wc_"
      case SnapshotGroup.Depth => lst + "_d_"
      case SnapshotGroup.Redundancy => lst + "_rr_"
    }

    val intvs = intervals

    println("loaded partitions: " + data.rdd.getNumPartitions)

    //split, adding the string that specifies the range
    val dfs = split match {
      case SnapshotGroup.None => Seq((data, pname + intervals.head.toString.drop(1).dropRight(1)))
      case _ =>
        splitRDD(data.rdd.flatMap{r => 
          val ii = Interval(r.getDate(startIndex), r.getDate(startIndex+1))
          intvs.filter(intvs => intvs.intersects(ii)).map(intv => Row.fromSeq(r.toSeq.patch(startIndex, Seq(Date.valueOf(intv.intersection(ii).get.start), Date.valueOf(intv.intersection(ii).get.end)), 2)))}, intervals, startIndex).map(d => ProgramContext.getSession.createDataFrame(d, data.schema))
          .zip(intervals).map(x => (x._1, pname + x._2.toString.drop(1).dropRight(1)))
    }

    //convert the sqldate to seconds
    val converted = dfs.map(x => (x._1.withColumn("estart", x._1("estart").cast(TimestampType).cast(LongType).as("estart")).withColumn("eend", x._1("eend").cast(TimestampType).cast(LongType).as("eend")), x._2))

    //sort
    val sorted = locality match {
      case Locality.Temporal => converted.map(x => (x._1.orderBy(col(idname), col("estart")), x._2))
      case Locality.Structural => converted.map(x => (x._1.orderBy(col("estart"), col(idname)), x._2))
    }

    //now write each
    //val numParts = data.rdd.getNumPartitions
    sorted.foreach{ x => 
      val pth = dest + x._2 + "tmp"
      //write out as is, then reload and coalesce into fewer files
      x._1.write.parquet(pth)
      val fs = FileSystem.get(ProgramContext.sc.hadoopConfiguration)
      val dirSize = fs.getContentSummary(new Path(pth)).getLength 
      val parts = math.max(32, dirSize / blocksize).toInt
      val df = ProgramContext.getSession.read.parquet(pth)
      df.coalesce(parts).write.parquet(dest + x._2)
      fs.delete(new Path(pth), true)
    }

  }

  private def splitRDD(df: RDD[Row], intervals: Seq[Interval], startIndex: Int): Seq[RDD[Row]] = {
    val intvs = intervals.zipWithIndex
    val mux = df.mapPartitions{itr =>
      val buckets = Vector.fill(intervals.size) { ArrayBuffer.empty[Row] }
      itr.foreach { r => intvs.filter(y => y._1.intersects(Interval(r.getDate(startIndex), r.getDate(startIndex+1)))).foreach(y => buckets(y._2) += r)} //should only go into one bucket if the split was done properly
      Iterator.single(buckets)
    }
    Vector.tabulate(intervals.size) { j => mux.mapPartitions { itr => itr.next()(j).toIterator }}
  }
}
