package edu.drexel.cs.dbgroup.portal.evaluation

import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.time.LocalDate
import scala.collection.JavaConverters._

import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.TripletFields
import org.apache.spark.{SparkConf,SparkContext}

import edu.drexel.cs.dbgroup.portal._
import edu.drexel.cs.dbgroup.portal.util.GraphLoader
import edu.drexel.cs.dbgroup.portal.representations.{HybridGraph,RepresentativeGraph}

/**
  * Calculates various evolution measures for the input dataset.
  * 1. Rate of densification / densification exponent
  * 2. Average distance between nodes
  * 3. Effective graph diameter
  * 4. Size of the largest weakly connected component, number of components, size of the largest component as a percentage of nodes
  * 5. Clustering coefficient
  * 6. Edge reciprocity
*/

object Measures {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    var conf = new SparkConf().setAppName("TemporalGraph Project").setSparkHome(System.getenv("SPARK_HOME"))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.network.timeout", "240")   
    val sc = new SparkContext(conf)
    ProgramContext.setContext(sc)
    val sqlContext = ProgramContext.getSession
    sqlContext.conf.set("spark.sql.files.maxPartitionBytes", "16777216")
    sqlContext.emptyDataFrame.count

    val dataset = args(1)
    val analysis = args(0).toInt

    analysis match {
      case 0 | 1 => runDensification(dataset)
      case 0 | 2 => runDistance(dataset)
      case 0 | 3 => runDiameterL(dataset)
      case 0 | 4 => runComponent(dataset)
      case 0 | 5 => runClusteringC(dataset)
      case 0 | 6 => runReciprocity(dataset)
    }
  }

  def runDensification(data:String): Unit = {
    //we output the number of nodes as a function of time
    //and the number of edges as a function of time

    val g = GraphLoader.buildVE(data, -1, -1, Interval(LocalDate.MIN,LocalDate.MAX))
    val agg = g.vmap((vid, intv, attr) => 1, 0).createAttributeNodes((a,b) => a + b)((vid,attr) => 1L)
    //calculate number of edges per node
    val agg2 = agg.aggregateMessages[Int](sendMsg = (et => Iterator((et.dstId,1))), (a,b) => a+b, 0, TripletFields.None)
    //now each node property is a tuple (number of nodes, number of edges)
    println("distribution of nodes and edges with time:")
    println(agg2.allVertices.collect.mkString("\n"))
  }

  def runDistance(data:String): Unit = {
    //run shortest path computation for all pairs of nodes
    val g = GraphLoader.buildHG(data, -1, -1, Interval(LocalDate.MIN, LocalDate.MAX))
    //there are too many nodes to pass them all as landmarks
    //so do this in stages, 10K nodes at a time
    val allNodeIds = g.vertices.map{ case (vid, a) => vid}.distinct
    val maxNodeId: Long = allNodeIds.max
    var low: Long = 0L
    println("maxNodeId: " + maxNodeId)
    var union = HybridGraph.emptyGraph[(Long, Long), Any]((0L,0L))
    (0L to (maxNodeId/10000+1)*10000 by 10000).tail.foreach {x =>
      val subset = allNodeIds.filter(id => id >= low && id < x).collect
      low = x
      println("next x: " + x)
      val sd = g.shortestPaths(true, subset.toSeq)
      //need to throw away self-distance and replace MAX with 0
      //reduce each map to a sum and count
      val cleaned = sd.vmap((vid, intv, mp) => { val fil = mp._2.asScala.toSeq.filter{ case (k,v) => v > 0}; if (fil.size > 0) fil.map{ case (k,v) => if (v == Int.MaxValue) (0L, 1L) else (v.toLong, 1L)}.reduce((a,b) => (a._1 + b._1, a._2 + b._2)) else (0L,0L)}, (0L,0L))
      //coalesce into one node per time instant
      val agg = cleaned.createAttributeNodes((a,b) => (a._1 + b._1, a._2 + b._2))((vid, attr) => 1L)
      //now union with all the prev ones to get a single one
      union = union.union(agg, (a,b) => (a._1+b._1, a._2+b._2), (a,b) => a)
    }
    //now union contains the sum and count of distances for each time period
    //compute average
    println("average path length:")
    println(union.vmap((vid, intv, attr) => attr._1 / (attr._2 * (attr._2-1)), 0.0).allVertices.collect.mkString("\n"))
  }

  def runDiameterE(data:String): Unit = {
    //effective graph diameter is the the value where 90% of values fall within it
    //i.e. using cumulative distribution
    val g = GraphLoader.buildHG(data, -1, -1, Interval(LocalDate.MIN, LocalDate.MAX))
    //there are too many nodes to pass them all as landmarks
    //so do this in stages, 10K nodes at a time
    val allNodeIds = g.vertices.map{ case (vid, a) => vid}.distinct
    val maxNodeId: Long = allNodeIds.max
    var low: Long = 0L
    println("effective diameter data")
    (0L to (maxNodeId/10000+1)*10000 by 10000).tail.foreach {x =>
      val subset = allNodeIds.filter(id => id >= low && id < x).collect
      low = x
      val sd = g.shortestPaths(true, subset.toSeq)
      val cleaned: TGraphNoSchema[Seq[(Long,Long,Int)],Any] = sd.vmap( (vid, intv, mp) => mp._2.asScala.toSeq.filter{ case (k,v) => v > 0 && v < Int.MaxValue}.map{ case (k,v) => (vid,k,v)}, Seq[(Long,Long,Int)]())
      val agg = cleaned.createAttributeNodes((a,b) => a ++ b)((vid,attr) => 1L)
      //now each node is a collection of all the pairwise distances for that time period
      println(agg.vertices.flatMap{ case (vid, (intv, mp)) => mp.map{ case (k1,k2,v) => (intv, k1, k2, v)}}.collect.mkString("\n"))
    }
  }

  def runDiameterL(data:String): Unit = {
    //graph diameter is the length of the longest graph path
    val g = GraphLoader.buildHG(data, -1, -1, Interval(LocalDate.MIN, LocalDate.MAX))
    //there are too many nodes to pass them all as landmarks
    //so do this in stages, 10K nodes at a time
    val allNodeIds = g.vertices.map{ case (vid, a) => vid}.distinct
    val maxNodeId: Long = allNodeIds.max
    var low: Long = 0L
    //var union = HybridGraph.emptyGraph[Int, Any](0)
    println("maxnodeId: " + maxNodeId)
    println("graph diameter:")
    (0L to (maxNodeId/10000+1)*10000 by 10000).tail.foreach {x =>
      val subset = allNodeIds.filter(id => id >= low && id < x).collect
      println("next x: " + x)
      low = x
      val sd = g.shortestPaths(true, subset.toSeq)
      //now we just need the longest path for each
      val cleaned = sd.vmap((vid, intv, attr) => { val t = attr._2.asScala.values.filter{ v => v > 0 && v < Int.MaxValue}; if (t.size > 0) t.reduce(math.max) else 0}, 0)
      val agg = cleaned.createAttributeNodes(math.max)((vid,attr) => 1L)
      println(agg.allVertices.collect.mkString("\n"))
      //now union with all the prev ones to get a single one
      //union = union.union(agg, (a,b) => math.max(a,b), (a,b) => a)
    }    
    //println(union.allVertices.collect.mkString("\n"))
  }

  def runComponent(data:String): Unit = {
    //compute the size of the largest weakly connected component for each time instant
    //as well as the number of components and percent of nodes in the largest component
    val g = GraphLoader.buildHG(data, -1, -1, Interval(LocalDate.MIN, LocalDate.MAX))
    val conn = g.connectedComponents()
    val cleaned = conn.vmap((vid, intv, attr) => (attr._2, 1L), (0L, 0L))
    val agg = cleaned.createAttributeNodes((a,b) => (a._1, a._2+b._2))((vid,attr) => attr._1)
    //now each node is one connected component with size as the second attribute
    //val agg2 = agg.vmap((vid,intv,attr) => (attr._2, attr._2, 1), (0L,0L,0)).createAttributeNodes((a,b) => (math.max(a._1,b._2), a._2 + b._2, a._3 + b._3))((vid,attr) => 1L)
    //val fin = agg2.vmap((vid,intv,attr) => (attr._1, attr._2, attr._3, attr._1.toDouble/attr._2), (0L,0L,0,0.0))
    //now there is a single node per time interval, with four attributes:
    //max component size, total number of nodes, total number of components, and percentage of nodes in largest component
    println("weakly connected components: (largest size, N, C, %)")
    //println(fin.vertices.collect.mkString("\n"))
    val rowRdd = agg.allVertices.map(x => Row(x._1, java.sql.Date.valueOf(x._2._1.start), java.sql.Date.valueOf(x._2._1.end), x._2._2._2))
    val schema = StructType(StructField("component", LongType, false) ::
      StructField("estart", DateType, false) ::
      StructField("eend", DateType, false) ::
      StructField("size", LongType, false) :: Nil)
    val frame = ProgramContext.getSession.createDataFrame(rowRdd, schema)
    frame.groupBy("estart", "eend").agg(max("size"),sum("size"),count("component")).show(500)
  }

  def runClusteringC(data:String): Unit = {
    //the mean clustering coefficient
    val g = GraphLoader.buildHG(data, -1, -1, Interval(LocalDate.MIN, LocalDate.MAX))
    val tri = g.clusteringCoefficient().vmap((vid,intv,attr) => (attr._2, 1), (0.0,0))
    //now need to compute the average
    val agg = tri.createAttributeNodes((a,b) => (a._1+b._1,a._2+b._2))((vid,attr) => 1L)
    val clean = agg.vmap((vid,intv,attr) => attr._1/attr._2, 0.0)
    println("mean clustering coefficient:")
    println(clean.allVertices.collect.mkString("\n"))
  }

  def runReciprocity(data:String): Unit = {
    //reciprocity is the percent of edges that have a reciprocal edge
    val g = GraphLoader.buildHG(data, -1, -1, Interval(LocalDate.MIN, LocalDate.MAX))
    //one way to compute this is to turn all edges into canonical form
    //and then reduce, counting. 2=reciprocal pair, otherwise 1
    //another way is to use aggregatemessages to smaller node id
    val agm = g.aggregateMessages[Seq[Long]](sendMsg = (et => {
      if (et.srcId < et.dstId)
        Iterator((et.srcId, Seq(et.dstId)))
      else
        Iterator((et.dstId, Seq(et.srcId)))
    }), (a,b) => a++b, Seq[Long](), TripletFields.None)
    //now each vertex has an attribute that is a sequence of ids of larger neighbors
    //we just want to count total and repeats
    val recip = agm.vmap((vid, intv, attr) => {
      val total = attr._2.size.toLong
      val r = attr._2.groupBy(identity).map(x => x._2.size).filter(_ > 1)
      if (r.size > 0) 
         (total, r.reduce(_+_).toLong)
      else
         (total,0L)
    }, (0L,0L))
    //now combine for each time period
    val agg = recip.createAttributeNodes((a,b) => (a._1+b._1, a._2+b._2))((vid,attr) => 1L)
    //the ratio
    val res = agg.vmap((vid,intv,attr) => attr._2.toDouble/attr._1, 0.0)
    println("reciprocity:")
    println(res.allVertices.collect.mkString("\n"))
  }

}

