package xyz.flysium.lib

import org.apache.spark.graphx._
import org.slf4j.{Logger, LoggerFactory}
import xyz.flysium.utils.GraphUtils

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * 有向图的环输出
 *
 * @author zeno
 */
case object DirectGraphCyclesSearch extends Serializable {

  var log: Logger = LoggerFactory.getLogger(DirectGraphCyclesSearch.getClass)
  var printSwitch = false

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int): Array[ArrayBuffer[VertexId]] = {
    // query clusters of stronglyConnectedComponents
    val clusters = graph.stronglyConnectedComponents(numIter).vertices
      .map(x => (x._2, Array(x._1)))
      .reduceByKey((a, b) => a ++ b)
      .filter(x => x._2.length > 1)
      .collect()

    // foreach find cycles
    clusters
      .map(cluster => {
        if (printSwitch) {
          log.info(s"cluster => ${cluster._2.mkString(" ")}")
        }
        var subgraph = graph.subgraph(vd => cluster._2.contains(vd.srcId))
        subgraph = GraphUtils.removeSingletons(subgraph)
        if (printSwitch) {
          log.info(s"subgraph => ${subgraph.vertices.map(x => x._1).collect().mkString(" ")}")
        }
        // find cycles in a single cluster of stronglyConnectedComponents
        cyclesSearch(subgraph, numIter)
      })
      .flatMap(c => c.iterator)
  }

  // find cycles in a single cluster of stronglyConnectedComponents
  private def cyclesSearch[VD: ClassTag, ED: ClassTag](initGraph: Graph[VD, ED], numIter: Int): Array[ArrayBuffer[VertexId]] = {
    // upstream `list of` VertexId path
    case class Msg(upstreamPath: ArrayBuffer[VertexId] = ArrayBuffer()) {}

    // list of VertexId path
    case class PathAttr(paths: ArrayBuffer[ArrayBuffer[VertexId]] = ArrayBuffer()) {}

    type A = ArrayBuffer[Msg]
    type SVD = PathAttr

    val graph: Graph[SVD, ED] = initGraph.mapVertices((_, _) => PathAttr())

    /**
     * 初始化msg，每个节点属性都初始化
     *
     * @return 空的msg
     */
    def initialMsg(): A = {
      ArrayBuffer(Msg())
    }

    /**
     * 将收到的消息更新到当前节点属性
     *
     * @param vId   点id
     * @param vd    点属性
     * @param input 发送过来的消息
     * @return
     */
    def vprog(vId: VertexId, vd: SVD, input: A): SVD = {
      if (printSwitch) {
        log.info(s"$vId receive message: ${input.map(m => m.upstreamPath.mkString("->")).mkString(" ")}")
      }

      val paths = ArrayBuffer[ArrayBuffer[VertexId]]()
      var valid = false
      input.foreach(msg => {
        if (msg.upstreamPath.nonEmpty) {
          if (vd.paths.isEmpty) {
            paths.append(msg.upstreamPath)
          } else {
            val newPath = ArrayBuffer[VertexId]()
            newPath.appendAll(msg.upstreamPath)
            newPath.append(vId)
            paths.append(newPath)
          }
          valid = true
        }
      })
      if (valid) {
        if (printSwitch) {
          log.info(s"$vId after message: ${paths.map(m => m.mkString("->")).mkString(" ")}")
        }
        PathAttr(paths.distinct)
      } else {
        vd
      }
    }

    def sendMsg(edgeTriplet: EdgeTriplet[SVD, ED]): Iterator[(VertexId, A)] = {
      var iter: Iterator[(VertexId, A)] = Iterator.empty
      if (!edgeTriplet.dstAttr.paths.exists(path => path.count(e => e == edgeTriplet.srcId) > 1)) {
        val msg = ArrayBuffer[Msg]()
        if (edgeTriplet.srcAttr.paths.isEmpty) {
          msg.append(Msg(ArrayBuffer(edgeTriplet.srcId, edgeTriplet.dstId)))
        } else {
          edgeTriplet.srcAttr.paths.foreach(path => {
            msg.append(Msg(path))
          })
        }
        iter = Iterator((edgeTriplet.dstId, msg))
      }
      iter
    }

    def mergeMsg(a1: A, a2: A): A = {
      ArrayBuffer.concat(a1, a2).distinct
    }

    val result = Pregel(graph, initialMsg(), numIter, EdgeDirection.Out)(vprog, sendMsg, mergeMsg)
    if (printSwitch) {
      result.vertices.foreach(x => log.info(s"search result => Vertex ${x._1} , paths: ${x._2.paths.map(p => p.mkString("->")).mkString(" ")}"))
    }
    result.vertices
      .map(v => v._2.paths
        .filter(path => path.count(e => e == v._1) >= 2)
      )
      .flatMap(paths => paths.iterator)
      .distinct()
      //.filter(path => path.count(e => e == path.last) >= 2)
      .map(path => {
        // slice the cycle array buffer according by last element
        val list = sliceCycleByLast(path)
        // find index of minimum Vertex
        val minIndex = indexOfMinimumVertex(list)
        // restructure the array buffer from start index to other locations
        restructure(list, minIndex)
      })
      .distinct()
      .collect()
  }

  // slice the cycle array buffer according by last element
  // input:  2->3->4->5->1->2->3
  // output: 3->4->5->1->2
  private def sliceCycleByLast(path: ArrayBuffer[VertexId]): ArrayBuffer[VertexId] = {
    val start = path.indexOf(path.last)
    if (start < 0) {
      return ArrayBuffer()
    }
    val list = ArrayBuffer[VertexId]()
    for (i <- start until path.length - 1) {
      list.append(path(i))
    }
    list
  }

  // restructure the array buffer from start index to other locations
  // input:  3->4->5->1->2
  // output: 1->2->3->4->5
  private def restructure(path: ArrayBuffer[VertexId], startIndex: Int): ArrayBuffer[VertexId] = {
    val list = ArrayBuffer[VertexId]()
    for (i <- startIndex until path.length) {
      list.append(path(i))
    }
    for (i <- 0 until startIndex) {
      list.append(path(i))
    }
    list
  }

  // find index of minimum Vertex
  private def indexOfMinimumVertex(path: ArrayBuffer[VertexId]): Int = {
    var minIndex = 0
    for (index <- path.indices) {
      if (path(index) < path(minIndex)) {
        minIndex = index
      }
    }
    minIndex
  }

}


