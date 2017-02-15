import java.io.{FileWriter, BufferedWriter}

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import scala.collection.JavaConverters._

object GraphPartitioningTradeoff {
  def main(args: Array[String]) {
    val partitionStrategies = List(None, Some(PartitionStrategy.RandomVertexCut), Some(PartitionStrategy.EdgePartition1D), Some(PartitionStrategy.EdgePartition2D))
    if (args.size < 4) {
      print("Usage = ./run.sh [application] [graph file path] [partitioning strategy] [numItereations]")
    }
    val algorithm = args(0)
    val graphFilePath = args(1)
    val partitionerName = args(2)
    val partitioner = partitionerName match {
      case "random" => Some(PartitionStrategy.RandomVertexCut)
      case "1d" => Some(PartitionStrategy.EdgePartition1D)
      case "2d" => Some(PartitionStrategy.EdgePartition2D)
      case "none" => None
    }

    val numIterations = args(3).toInt

    runGraphAlgorithm(algorithm, partitioner, graphFilePath, numIterations)
  }

  def runGraphAlgorithm(algorithm: String, partitionStrategy: Option[PartitionStrategy], graphFilePath: String, numIterations: Int): Unit = {
    println(s"Running Graph Algorithm $algorithm with Partitioning Strategy: ${partitionStrategy.toString}, for graph: $graphFilePath, with numIterations: $numIterations")
    val conf = new SparkConf().setAppName("Graph Partitioning Tradeoff")
    val sc = new SparkContext(conf)
    val initialTimestamp: Long = System.currentTimeMillis

    var graph = GraphLoader.edgeListFile(sc, graphFilePath)

    val graphLoadedTimestamp: Long = System.currentTimeMillis
    val graphLoadingTime: Long = graphLoadedTimestamp - initialTimestamp
    println(s"Graph loading time: $graphLoadingTime")

    if (partitionStrategy.isDefined) {
      graph = graph.partitionBy(partitionStrategy.get)
      graph.edges.foreachPartition(x => {})
    }
    val graphPartitioningDoneTimestamp: Long = System.currentTimeMillis
    val graphPartitioningTime: Long = graphPartitioningDoneTimestamp - graphLoadedTimestamp
    println(s"Graph partitioning time: $graphPartitioningTime")

    // Run graph algorithm
    if (algorithm.equals("PageRank")) {
      PageRank.run(graph, numIterations)
    } else if (algorithm.equals("ShortestPaths")) {
      ShortestPaths.run(graph, graph.vertices.takeSample(true, numIterations).map(v => v._1))
    } else {
      throw new IllegalArgumentException(s"Invalid algorithm is selected: $algorithm")
    }
    val graphComputationDoneTimestamp: Long = System.currentTimeMillis
    val graphComputationTime: Long = graphComputationDoneTimestamp - graphPartitioningDoneTimestamp
    println(s"Graph computation time: $graphComputationTime")

    val totalTime: Long = graphComputationDoneTimestamp - initialTimestamp
    println(s"Total time: $totalTime")
    sc.stop()
  }
}
