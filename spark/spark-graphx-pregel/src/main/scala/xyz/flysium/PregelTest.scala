package xyz.flysium

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * @author zeno
 */
object PregelTest extends Serializable {

  def main(args: Array[String]): Unit = {
    // 屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf()
    conf.setAppName("PregelTest").setMaster("local")
    val sc = new SparkContext(conf)

    // sample
    val vertexArray = Array(
      (1L, (1, BigDecimal("100"))), (2L, (2, BigDecimal("100"))), (3L, (3, BigDecimal("60"))), (4L, (4, BigDecimal("20"))), (5L, (5, BigDecimal("40"))),
      (6L, (6, BigDecimal("25"))), (7L, (7, BigDecimal("20"))), (8L, (8, BigDecimal("60"))), (9L, (9, BigDecimal("30"))), (10L, (10, BigDecimal("10")))
    )
    val edgeArray = Array(
      Edge(1L, 3L, 1), Edge(2L, 3L, 1), Edge(3L, 4L, 1), Edge(3L, 5L, 1), Edge(3L, 10L, 1),
      Edge(5L, 6L, 1), Edge(6L, 8L, 1), Edge(8L, 7L, 1),
      Edge(8L, 9L, 1)
    )
    val vertexRDD: RDD[(Long, (Int, BigDecimal))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    // build graph
    val graph: Graph[(Int, BigDecimal), Int] = Graph(vertexRDD, edgeRDD)

    // class WAction
    case class WAction(
                        // ID
                        actionId: Int,
                        // inDegree for the vertex
                        inDeg: Int,
                        // outDegree for the vertex
                        outDeg: Int,
                        // graph pregel active status
                        active: Int,
                        // WAction cost
                        cost: BigDecimal)

    // transforms to WAction graph by join inDegrees, outDegrees
    val initialActionGraph: Graph[WAction, Int] = graph.mapVertices { case (_, (actionId, cost)) => WAction(actionId, 0, 0, 0, cost) }
    val actionGraph: Graph[WAction, Int] = initialActionGraph.outerJoinVertices(initialActionGraph.inDegrees) {
      case (_, a, inDegOpt) =>
        var active: Int = 0
        // set the vertex active which inDeg = 0
        if (inDegOpt.getOrElse(0) == 0) {
          active = 1
        }
        WAction(a.actionId, inDegOpt.getOrElse(0), a.outDeg, active, a.cost)
    }.outerJoinVertices(initialActionGraph.outDegrees) {
      case (_, a, outDegOpt) => WAction(a.actionId, a.inDeg, outDegOpt.getOrElse(0), a.active, a.cost)
    }

    //actionGraph.vertices.collect.foreach(v => println(s"${v._2.actionId} inDeg: ${v._2.inDeg}  outDeg: ${v._2.outDeg} active: ${v._2.active} cost: ${v._2.cost}"))

    // pregel
    val pregelGraph = actionGraph.pregel(
      // initialMsg – the message each vertex will receive at the on the first iteration
      BigDecimal("0"),
      // maxIterations – the maximum number of iterations to run for
      Int.MaxValue,
      // activeDirection – the direction of edges incident to a vertex that received a message in the previous round on which to run sendMsg. For example, if this is EdgeDirection.Out, only out-edges of vertices that received a message in the previous round will run.
      EdgeDirection.Out
    )(
      // vprog – the user-defined vertex program which runs on each vertex and receives the inbound message and computes a new vertex value. On the first iteration the vertex program is invoked on all vertices and is passed the default message. On subsequent iterations the vertex program is only invoked on those vertices that receive messages.
      (_: VertexId, vd: WAction, inputCost: BigDecimal) => {
        var active: Int = vd.active
        if (active == 0 && inputCost > 0) {
          active = 1
        }
        val outputCost = vd.cost + inputCost
        // println(s"Vertex $vid, cost: ${vd.cost}, receive one message=> cost: $inputCost. result=> $outputCost")
        WAction(vd.actionId, vd.inDeg, vd.outDeg, active, outputCost)
      },
      // sendMsg – a user supplied function that is applied to out edges of vertices that received messages in the current iteration
      (edgeTriplet: EdgeTriplet[WAction, Int]) => {
        if (edgeTriplet.srcAttr.active == 1) {
          val sendCost = edgeTriplet.srcAttr.cost / edgeTriplet.srcAttr.outDeg
          // println(s"Vertex ${edgeTriplet.srcId} send message to Vertex ${edgeTriplet.dstId}. message=> cost: $sendCost")
          Iterator[(VertexId, BigDecimal)]((edgeTriplet.dstId, sendCost))
        } else {
          Iterator.empty
        }
      },
      // mergeMsg – a user supplied function that takes two incoming messages of type A and merges them into a single message of type A. This function must be commutative and associative and ideally the size of A should not increase.
      (cost1: BigDecimal, cost2: BigDecimal) => {
        cost1 + cost2
      }
    )

    pregelGraph.vertices.collect.foreach(v => println(s"${v._2.actionId} inDeg: ${v._2.inDeg}  outDeg: ${v._2.outDeg} active: ${v._2.active} cost: ${v._2.cost}"))
  }

}
