package job.local.graphx

import job.local.graphx.Types._
import org.apache.spark.graphx._

import scala.reflect.ClassTag

object MinimalSpanningForest extends Logger {

    def apply[VD : ClassTag] (
                               graph : Graph[VD, EdgeWeight],
                               maxNumIterations : Int = Int.MaxValue,
                               isConnected : Boolean = false
                             ) : Graph[(VertexId, VertexId), Boolean] = {

        // a vertex's attribute is the id of the other vertex at its minimal incident edge
        val vRDD = graph.aggregateMessages[(VertexId, EdgeWeight)](
            ctx => ctx.sendToSrc((ctx.dstId, ctx.attr)), {
                case ((v1, w1), (v2, w2)) =>
                    if (w1 > w2) (v2, w2) else (v1, w1)
            })
        vRDD.collect().foreach(v => info("Vertex(" + v._1 + ", (" + v._2._1 + ", " + v._2._2 + "))"))

        val superVertexGraph = Graph(vRDD.mapValues[VertexId]((v : (VertexId, EdgeWeight)) => v._1), graph.edges)
        superVertexGraph.vertices.collect().foreach(v => info("Vertex(" + v._1 + ", " + v._2 + ")"))

        val g = Graph(superVertexGraph.aggregateMessages[VertexId](
            ctx => {
                if (ctx.dstId == ctx.srcAttr && ctx.dstAttr == ctx.srcId) {
                    ctx.sendToDst(if (ctx.dstId < ctx.srcId) ctx.dstId else ctx.dstAttr)
                    ctx.sendToSrc(if (ctx.dstId < ctx.srcId) ctx.srcAttr else ctx.srcId)
                } else {
                    ctx.sendToDst(ctx.dstAttr)
                    ctx.sendToSrc(ctx.srcAttr)
                }
            },
            (vid1, vid2) => math.min(vid1, vid2)
        ), graph.edges)

        val msfGraph = {
            if (isConnected) g
            else graph.outerJoinVertices(g.vertices) {
                (vid, data, opt) => opt.getOrElse(vid)
            }
        }.mapVertices[(VertexId, VertexId)] {
            (vid, parent) => (if (parent == vid) vid else -1, parent)
        }.mapTriplets {
            e => e.srcAttr._2 == e.dstId || e.dstAttr._2 == e.srcId
        }
        Pregel(msfGraph, -1L)(
            (vid, attr, cid) => (if (cid > 0) cid else attr._1, attr._2),
            e => {
                if (e.attr) {
                    if (e.dstAttr._2 == e.srcId && e.srcAttr._1 > 0 && e.dstAttr._1 < 0)
                        Iterator((e.dstId, e.srcAttr._1))
                    else if (e.srcAttr._2 == e.dstId && e.srcAttr._1 < 0 && e.dstAttr._1 > 0)
                        Iterator((e.srcId, e.dstAttr._1))
                    else if (e.srcAttr._1 < 0 || e.dstAttr._1 < 0)
                        Iterator((e.srcId, -1L), (e.dstId, -1L))
                }
                Iterator()
            },
            (a, _) => a
        )
    }
}