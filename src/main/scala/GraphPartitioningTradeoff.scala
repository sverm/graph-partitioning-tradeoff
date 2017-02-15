import java.io.{FileWriter, BufferedWriter}

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.util.collection.BitSet
import scala.collection.JavaConverters._
import scala.collection.mutable

case class IngressCorrectObliviousVertexCut(
  partitions: Int = -1,
  useHash: Boolean = false,
  useRecent: Boolean = false)
extends IngressEdgePartitioner {
  def numPartitions: Int = partitions
  def fromEdges[T <: Edge[_] : ClassTag](rdd: RDD[T]): RDD[T] = {
    val partNum = if (numPartitions > 0) numPartitions else rdd.partitions.size
    rdd.mapPartitions { iter =>
      val favorMap = (new mutable.HashMap[VertexId, BitSet]).withDefault( _ => new BitSet(partNum))
      val partNumEdges = new Array[Int](partNum)
      iter.toArray.map { e =>
        val srcFavor = favorMap(e.srcId)
        val dstFavor = favorMap(e.dstId)
        val part =
          getPartition(e.srcId, e.dstId, partNum, srcFavor, dstFavor,
            partNumEdges, useHash, useRecent)
        (part, e)
      }.toIterator
    }.partitionBy(new HashPartitioner(partNum)).map{ _._2 }
  }

  private def getPartition(srcId: VertexId, dstId: VertexId, partNum: Int,
    srcFavor: BitSet, dstFavor: BitSet, partNumEdges: Array[Int],
    useHash: Boolean, useRecent: Boolean): Int = {
      val epsilon = 1.0
      val minEdges = partNumEdges.min
      val maxEdges = partNumEdges.max
      val partScores =
        (0 until partNum).map { i =>
          val sf = srcFavor.get(i) || (useHash && (srcId % partNum == i))
          val tf = dstFavor.get(i) || (useHash && (dstId % partNum == i))
          val f = (sf, tf) match {
            case (true, true) => 2.0
            case (false, false) => 0.0
            case _ => 1.0
          }
          f + (maxEdges - partNumEdges(i)).toDouble / (epsilon + maxEdges - minEdges)
        }
      val maxScore = partScores.max
      val topParts = partScores.zipWithIndex.filter{ p =>
      math.abs(p._1 - maxScore) < 1e-5 }.map{ _._2}

      // Hash the edge to one of the best procs.
      val edgePair = if (srcId < dstId) (srcId, dstId) else (dstId, srcId)
      val bestPart = topParts(math.abs(edgePair.hashCode) % topParts.size)
      if (useRecent) {
        srcFavor.clear
        dstFavor.clear
      }
      srcFavor.set(bestPart)
      dstFavor.set(bestPart)
      partNumEdges(bestPart) = partNumEdges(bestPart) + 1
      bestPart
  }
}

object WCC {
  /**
   * Compute the connected component membership of each vertex and return a graph with the vertex
   * value containing the lowest vertex id in the connected component containing that vertex.
   *
   * @tparam VD the vertex attribute type (discarded in the computation)
   * @tparam ED the edge attribute type (preserved in the computation)
   *
   * @param graph the graph for which to compute the connected components
   *
   * @return a graph with vertex attributes containing the smallest vertex in each
   *         connected component
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], maxIterations: Int): Graph[VertexId, ED] = {
    val ccGraph = graph.mapVertices { case (vid, _) => vid }
    def sendMessage(edge: EdgeTriplet[VertexId, ED]) = {
      if (edge.srcAttr < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr))
      } else if (edge.srcAttr > edge.dstAttr) {
        Iterator((edge.srcId, edge.dstAttr))
      } else {
        Iterator.empty
      }
    }
    val initialMessage = Long.MaxValue
    Pregel(ccGraph, initialMessage, maxIterations, activeDirection = EdgeDirection.Either)(
      vprog = (id, attr, msg) => math.min(attr, msg),
      sendMsg = sendMessage,
      mergeMsg = (a, b) => math.min(a, b))
  } // end of connectedComponents
}

object SSSP {
  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type SPMap = Map[VertexId, Int]

  private def makeMap(x: (VertexId, Int)*) = Map(x: _*)

  private def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

  /**
   * Computes shortest paths to the given set of landmark vertices.
   *
   * @tparam ED the edge attribute type (not used in the computation)
   *
   * @param graph the graph for which to compute the shortest paths
   * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
   * landmark.
   *
   * @return a graph where each vertex attribute is a map containing the shortest-path distance to
   * each reachable landmark vertex.
   */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], landmarks: Seq[VertexId], maxIterations: Int): Graph[SPMap, ED] = {
    val spGraph = graph.mapVertices { (vid, attr) =>
      if (landmarks.contains(vid)) makeMap(vid -> 0) else makeMap()
    }

  val initialMessage = makeMap()

  def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
    addMaps(attr, msg)
  }

  def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
    val newAttr = incrementMap(edge.dstAttr)
    if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
    else Iterator.empty
  }

  Pregel(spGraph, initialMessage, maxIterations)(vertexProgram, sendMessage, addMaps)
  }
}


object GraphPartitioningTradeoff {
  def main(args: Array[String]) {
    if (args.size < 4) {
      print("Usage = ./run.sh [application] [graph file path] [partitioning strategy] [numItereations]")
    }
    val algorithm = args(0)
    val graphFilePath = args(1)
    val partitionerName = args(2)
    val partitioner = partitionerName match {
      case "random" => Some(new IngressRandomVertexCut)
      case "1d" => Some(new IngressEdgePartition1D)
      case "2d" => Some(new IngressEdgePartition2D)
      case "oblivious" => Some(new IngressCorrectObliviousVertexCut)
      case "none" => None
    }

    val numIterations = args(3).toInt

    runGraphAlgorithm(algorithm, partitioner, graphFilePath, numIterations)
  }

  def runGraphAlgorithm(algorithm: String, partitionStrategy: Option[IngressEdgePartitioner], graphFilePath: String, numIterations: Int): Unit = {
    println(s"Running Graph Algorithm $algorithm with Partitioning Strategy: ${partitionStrategy.toString}, for graph: $graphFilePath, with numIterations: $numIterations")
    val conf = new SparkConf().setAppName("Graph Partitioning Tradeoff")
    val sc = new SparkContext(conf)
    val initialTimestamp: Long = System.currentTimeMillis

    var graph = GraphLoader.edgeListFile(sc, graphFilePath, partitioner=partitionStrategy)
    graph.edges.foreachPartition(x => {}) // materialize

    val graphLoadedTimestamp: Long = System.currentTimeMillis
    val graphLoadingTime: Long = graphLoadedTimestamp - initialTimestamp
    println(s"Graph loading time: $graphLoadingTime")

    /*
    if (partitionStrategy.isDefined) {
      graph = graph.partitionBy(partitionStrategy.get)
      graph.edges.foreachPartition(x => {})
    }
    val graphPartitioningDoneTimestamp: Long = System.currentTimeMillis
    val graphPartitioningTime: Long = graphPartitioningDoneTimestamp - graphLoadedTimestamp
    println(s"Graph partitioning time: $graphPartitioningTime")
    */

    // Run graph algorithm
    if (algorithm.equals("PageRank")) {
      PageRank.run(graph, numIterations)
    } else if (algorithm.equals("WCC")) {
      WCC.run(graph, numIterations)
    } else if (algorithm.equals("ShortestPaths")) {
      SSSP.run(graph, graph.vertices.takeSample(true, 1).map(v => v._1), numIterations)
    } else {
      throw new IllegalArgumentException(s"Invalid algorithm is selected: $algorithm")
    }
    val graphComputationDoneTimestamp: Long = System.currentTimeMillis
    val graphComputationTime: Long = graphComputationDoneTimestamp - graphLoadedTimestamp
    println(s"Graph computation time: $graphComputationTime")

    val totalTime: Long = graphComputationDoneTimestamp - initialTimestamp
    println(s"Total time: $totalTime")
    sc.stop()
  }
}
