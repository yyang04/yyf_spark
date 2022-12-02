//package job.local
//
//
//import utils.SparkJobs.LocalSparkJob
//import org.apache.spark.graphx._
//import utils.PrivacyClustering.{Neighbor, VertexAttr}
//
//import scala.util.control.Breaks
//case class Neighbor (vertexId: Long, weight: Double)
//case class VertexAttr (parent: Long, neighbor: Neighbor)
//
//
//object AffinityClusteringTest extends LocalSparkJob {
//    // Edge(1,2,9), Edge(1,7,20), Edge(0,6,7),
//    override def run(): Unit = {
//        val nodes = sc.parallelize((0L to 8L).map(x => (x, VertexAttr(x, Neighbor(x, 0.0)))), 2)
//        var edges = sc.parallelize(
//            Seq(Edge(0,1,4), Edge(2,3,6), Edge(1,0,4), Edge(3,5,5), Edge(5,3,5), Edge(2,4,2),Edge(0,6,7),
//                Edge(4,3,10), Edge(4,5,15), Edge(4,8,5), Edge(4,7,1),
//                Edge(1,6,11), Edge(6,7,1), Edge(7,8,3), Edge(8,5,12))
//            , 100)
//          .map{ case Edge(src, dst, w) => Edge(src, dst, w.toDouble) }
//        // edges = edges.union(edges.map{ case Edge(src, dst, w) => Edge(dst, src, w) })
//        var graph = Graph(nodes, edges)
//        var mst = sc.emptyRDD[Edge[Int]]
//        Breaks.breakable {
//            for ( _ <- 0 until 3) {
//                graph = graph.joinVertices(
//                    graph.aggregateMessages[Neighbor](
//                        sendMsg = ctx =>
//                            if (ctx.dstAttr.parent != ctx.srcAttr.parent) {
//                                ctx.sendToSrc(Neighbor(ctx.dstId, ctx.attr))
//                                ctx.sendToDst(Neighbor(ctx.srcId, ctx.attr))
//                            },
//                        mergeMsg = {
//                            case (n1, n2) => if (n1.weight > n2.weight) n2 else n1
//                        })
//                )((_, attr1, attr2) => VertexAttr(attr1.parent, attr2))
//
//                mst = mst.union(
//                    Graph(
//                        vertices = graph.vertices,
//                        edges = graph.vertices.map { case (vid, VertexAttr(parent, _) ) => Edge(vid, parent, 0) }
//                    ).aggregateMessages[Neighbor](
//                        sendMsg = ctx => ctx.sendToDst(ctx.srcAttr.neighbor),
//                        mergeMsg = {
//                            case (n1, n2) => if (n1.weight > n2.weight) n2 else n1
//                        }
//                    ).map { case (vid, n) => Edge(vid, n.vertexId, 0) })
//
//
//                graph = graph.joinVertices(
//                    Graph(
//                        vertices = graph.vertices,
//                        edges = mst
//                    ).connectedComponents.vertices)((_, attr1, attr2) => VertexAttr(attr2, attr1.neighbor))
//                val count = graph.vertices.map{ case(_, attr) => (attr.parent, 1) }.reduceByKey(_ + _).collect().map(_._2)
//                if (count.exists(_ > 10)) Breaks.break()
//            }
//        }
//
//        graph.vertices.map{ case(vid, attr) => (vid, attr.parent)}.collect.foreach(println(_))
//
//
//
//
//
//
//
//
////        val new_mst = g.aggregateMessages[(Long, (Long, Int))](
////            sendMsg = ctx => ctx.sendToDst( (ctx.srcId, ctx.srcAttr) ),
////            mergeMsg = {
////                case ((v1, e1), (v2, e2)) =>
////                    if (e1._2 > e2._2) (v2, e2) else (v1, e1)
////            })
////          .map{ case(vid, (src, (dst, w))) => Edge(src, dst, w)}
////
////        mst = mst.union(new_mst)
////        val cc = Graph(nodes, mst).connectedComponents.vertices
////        graph = graph.joinVertices(cc)((_, _, attr) => attr)
////
////        vRDD = graph.aggregateMessages[(Long, Int)](
////            sendMsg = ctx =>
////                if (ctx.dstAttr != ctx.srcAttr) {
////                    ctx.sendToSrc( (ctx.dstId, ctx.attr) )
////                    ctx.sendToDst( (ctx.srcId, ctx.attr) )
////                },
////            mergeMsg = {
////                case ((v1, w1), (v2, w2)) =>
////                    if (w1 > w2) (v2, w2) else (v1, w1)
////            })
////
////        vRDD.collect.foreach(println(_))
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//        // 通过 cc 生成新图
//        // 原图 aggregate message，找到边和节点，边是聚合变换而来的，顶点就是节点
//        // 将这个图通过cc重新生成一张新图
//        // 然后 继续在新图上面 aggregate message
//        // 然后在cc上继续聚合找到core edge
//        // 然后在core edge 加入到 mst里面
//        // 然后在根据mst 的边继续cc，更新节点属性
//        // 在新图上面更新
//
//
//
//        // 用这个图构建
//
//
////        var newEdges = sc.emptyRDD[Edge[Int]]
////        var connectedGraph = Graph(nodes, newEdges)
////        var connectedEdges = connectedGraph.connectedComponents.vertices.map{ case(vid, cid) => Edge(vid, cid, 0) }
////        connectedGraph = Graph(nodes, connectedEdges)
////
////        // 新加入的边
////        var verticesTmp = graph.aggregateMessages[(Long, Int)](
////            sendMsg = ctx =>
////                if (ctx.dstAttr != ctx.srcAttr) {
////                    ctx.sendToSrc( (ctx.dstId, ctx.attr) )
////                    ctx.sendToDst( (ctx.srcId, ctx.attr) )
////                },
////            mergeMsg = {
////                case ((v1, w1), (v2, w2)) =>
////                if (w1 > w2) (v2, w2) else (v1, w1)
////            })
////
////        connectedGraph = Graph(nodes, verticesTmp.map{ case(vid, (dst, weight)) => Edge(vid, dst, 0) })
////        connectedEdges = connectedGraph.connectedComponents.vertices.map{ case(vid, cid) => Edge(vid, cid, 0) }
////
////        verticesTmp.
////
////        connectedEdges.collect.foreach(println(_))
////        verticesTmp.collect.foreach(println(_))
////        val graphTmp = Graph(verticesTmp, connectedEdges)
////        val edgesTmp = graphTmp.aggregateMessages[(Long, Int)](
////            sendMsg = ctx => ctx.sendToDst(ctx.srcAttr),
////            mergeMsg = {
////                case ((v1, w1), (v2, w2)) =>
////                    if (w1 > w2) (v2, w2) else (v1, w1)
////            })
////
////        edgesTmp.collect.foreach(println(_))
//
//
//
//
//
//
//
////        val edges = tmp.map{ case(src, dst) =>  () }
////
////
////
////          ///.map { case (vid, (dst, weight)) => Edge(vid, dst, weight) })
////
////        connectedGraph = Graph(nodes, newEdges)
//
//
//
//
//
//
//
////          .map{ case (vid, (dst, weight)) => Edge(vid, dst, 0) }
////        val g2 = Graph(nodes, eRDD)
////        val g3 = graph.outerJoinVertices(g2.connectedComponents().vertices)((vid, c, d) => d.getOrElse(-1))
////        g3.vertices.collect.foreach(println(_))
////        val superVertexGraph = Graph(vRDD.mapValues[VertexId]((v : (VertexId, Int)) => v._1), graph.edges)
////
////        val g = Graph(superVertexGraph.aggregateMessages[VertexId](
////            ctx => {
////                if (ctx.dstId == ctx.srcAttr && ctx.dstAttr == ctx.srcId) {  // 如果你是我最小的边并且发送给我们最小的id
////                    ctx.sendToDst(if (ctx.dstId < ctx.srcId) ctx.dstId else ctx.dstAttr)
////                    ctx.sendToSrc(if (ctx.dstId < ctx.srcId) ctx.srcAttr else ctx.srcId)
////                } else {
////                    ctx.sendToDst(ctx.dstAttr)
////                    ctx.sendToSrc(ctx.srcAttr)
////                }
////            },
////            (vid1, vid2) => math.min(vid1, vid2)
////        ), graph.edges)
////
////
////
////        val msfGraph = g.mapVertices[(VertexId, VertexId)] {
////            (vid, parent) => (if (parent == vid) vid else -1, parent)
////        }.mapTriplets {
////            e => e.srcAttr._2 == e.dstId || e.dstAttr._2 == e.srcId
////        }
////
////        msfGraph.triplets.collect.foreach(println(_))
////
////        val new_graph = Pregel(
////            graph = msfGraph,
////            initialMsg = -1L
////        )(
////            vprog = (vid, attr, cid) => (if (cid > 0) cid else attr._1, attr._2),
////            sendMsg = e => {
////                if (e.attr) {
////                    if (e.dstAttr._2 == e.srcId && e.srcAttr._1 > 0 && e.dstAttr._1 < 0)
////                        Iterator((e.dstId, e.srcAttr._1))
////                    else if (e.srcAttr._2 == e.dstId && e.srcAttr._1 < 0 && e.dstAttr._1 > 0)
////                        Iterator((e.srcId, e.dstAttr._1))
////                    else if (e.srcAttr._1 < 0 || e.dstAttr._1 < 0)
////                        Iterator((e.srcId, -1L), (e.dstId, -1L))
////                }
////                Iterator()
////            },
////            mergeMsg = (a, _) => a
////        )
////
////        new_graph.vertices.collect.foreach(println(_))
//    }
//}
