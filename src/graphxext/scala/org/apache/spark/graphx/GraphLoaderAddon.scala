package org.apache.spark.graphx

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}
import org.apache.spark.graphx.impl.EdgeRDDImpl
import java.time.LocalDate
import java.time.Period
import java.time.temporal.ChronoUnit

object GraphLoaderAddon {

  //like the regular graphx version except an edge line also has an int attribute
  def edgeListFile(
      sc: SparkContext,
      path: String,
      canonicalOrientation: Boolean = false,
      numEdgePartitions: Int = -1,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : Graph[Int, Int] =
  {
    // Parse the edge data table directly into edge partitions
    val lines =
      if (numEdgePartitions > 0) {
        sc.textFile(path, numEdgePartitions).coalesce(numEdgePartitions)
      } else {
        sc.textFile(path)
      }
    val edges = lines.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[Int, Int]
      iter.foreach { line =>
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          var attr = 0
          if(lineArray.length > 2){
            attr = lineArray{2}.toInt
          }
          if (canonicalOrientation && srcId > dstId) {
            builder.add(dstId, srcId, attr)
          } else {
            builder.add(srcId, dstId, attr)
          }
        }
      }
      Iterator((pid, builder.toEdgePartition))
    }.persist(edgeStorageLevel).setName("GraphLoader.edgeListFile - edges (%s)".format(path))

    GraphImpl.fromEdgePartitions(edges, defaultVertexAttr = 1, edgeStorageLevel = edgeStorageLevel,
      vertexStorageLevel = vertexStorageLevel)
  } // end of edgeListFile

  //multiple files in parallel
  def edgeListFiles(
      lines: RDD[(String, String)],
      res: Period,
      unit: ChronoUnit,
      start: LocalDate,
      canonicalOrientation: Boolean = false,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : Graph[Int, (Int, Int)] =
  {
    val edges = lines.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[(Int, Int), Int]
      iter.foreach { x =>
        val (filename, line) = x
        val dt = LocalDate.parse(filename.split('/').last.dropWhile(!_.isDigit).takeWhile(_ != '.'))
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          var attr = 0
          if(lineArray.length > 2){
            attr = lineArray{2}.toInt
          }
          if (canonicalOrientation && srcId > dstId) {
            builder.add(dstId, srcId, ((start.until(dt, unit) / res.get(unit)).toInt, attr))
          } else {
            builder.add(srcId, dstId, ((start.until(dt, unit) / res.get(unit)).toInt, attr))
          }
        }
      }
      Iterator((pid, builder.toEdgePartition))
    }.persist(edgeStorageLevel).setName("GraphLoader.edgeListFiles - edges (%s)".format(start.toString))

    GraphImpl.fromEdgePartitions(edges, defaultVertexAttr = 1, edgeStorageLevel = edgeStorageLevel,
      vertexStorageLevel = vertexStorageLevel)
  } // end of edgeListFiles

}
