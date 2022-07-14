package local
import org.apache.spark.graphx.{Edge, _}
import utils.LocalSparkJob

object Test extends LocalSparkJob {
    def main(args: Array[String]): Unit = {
        super.initSpark()
        val vertices = sc.makeRDD((0L to 9L).map((_, 0)))
        var edges = sc.makeRDD(Seq(
            Edge(0L, 1L, 4),
            Edge(0L, 6L, 7),
            Edge(1L, 2L, 9),
            Edge(1L, 6L, 11),
            Edge(1L, 7L, 20),
            Edge(2L, 3L, 6),
            Edge(2L, 4L, 2),
            Edge(3L, 4L, 10),
            Edge(3L, 5L, 5),
            Edge(4L, 5L, 15),
            Edge(4L, 7L, 1),
            Edge(4L, 8L, 5),
            Edge(5L, 8L, 12),
            Edge(6L, 7L, 1),
            Edge(7L, 8L, 3)
        )).map{
            case Edge(srcId, dstId, weight) => Edge(srcId, dstId, weight.toDouble)
        }

        val reverseEdge = edges.map{ case Edge(src, dst, weight) => Edge(dst, src, weight)}
        edges = edges.union(reverseEdge)

        val g = Graph(vertices, edges)
        val v = g.aggregateMessages[Int](
            sendMsg = x => x.sendToDst(x.srcId.toInt),
            mergeMsg = _ + _
        )
        v.collect.foreach(println(_))
        // val g2 = g.mapTriplets(triplet => (triplet.attr, true))
//        g2.
        //g.triplets.collect.foreach(println(_))

//        val vertexRDD = g.aggregateMessages[Int](sendMsg= ctx => ctx.sendToDst(1), mergeMsg = _ + _)
//
//        g = g.outerJoinVertices(vertexRDD)((vid, vd, outD)=> {
//            outD match {
//                case Some(outD) => outD
//                case None => 0
//            }
//        })

        // g.vertices.collect().foreach(println(_))
//        var g2 = g.mapEdges(e => (e.attr,true))


//
//        println(g2.subgraph(_.attr._2).edges.collect().mkString)

        // val connected_components = g.connectedComponents().vertices.collect() (return id, components)
        // val new_g = g.mapVertices((id, _) => 1.0) // return (id, 1.0)
        // val new_g = g.mapEdges(e => e.attr.toDouble)
        // val new_g = g.subgraph(vpred = (vid, attr) => vid > 5)  // 取出节点大于5的子图
        // val new_g = g.subgraph(epred = triplet => triplet.attr > 5)  // 取出边大于5的子图
//
//        val vertex = g.mapVertices((id, _) => 1).aggregateMessages[Int](
//            triplet => {
//                triplet.sendToDst(1)
//            },
//            (a, b) => a + b
//        )
//        println(vertex.collect().mkString)
        // println(minSpanningTree(g).triplets.map(et => (et.srcAttr,et.dstAttr)).collect.mkString)
        // println(new_g.edges.collect().mkString)

    }

    def minSpanningTree[VD:scala.reflect.ClassTag](g:Graph[VD,Double]): Graph[VD, Double] = {
        var g2 = g.mapEdges(e => (e.attr,false))

        for (_ <- 1L to g.vertices.count-1) {

            val unavailableEdges =
                g2.outerJoinVertices(
                    g2.subgraph(_.attr._2)
                      .connectedComponents
                      .vertices)((vid, vd, cid) => (vd,cid))
                  .subgraph(et => et.srcAttr._2.getOrElse(-1) == et.dstAttr._2.getOrElse(-2))
                  .edges.map(e => ((e.srcId, e.dstId), e.attr))
            type edgeType = ((VertexId, VertexId), Double)

            val smallestEdge =
                g2.edges
                  .map(e => ((e.srcId, e.dstId), e.attr))
                  .leftOuterJoin(unavailableEdges)
                  .filter(x => !x._2._1._2 && x._2._2.isEmpty)
                  .map(x => (x._1, x._2._1._1))
                  .min()(new Ordering[edgeType]() {
                      override def compare(a:edgeType, b:edgeType): PartitionID = {
                          val r = Ordering[Double].compare(a._2,b._2)
                          if (r == 0)
                              Ordering[Long].compare(a._1._1, b._1._1)
                          else
                              r }
                  })

            g2 = g2.mapTriplets(et =>
                (et.attr._1, et.attr._2 || (et.srcId == smallestEdge._1._1
                  && et.dstId == smallestEdge._1._2)))
        }
        g2.subgraph(_.attr._2).mapEdges(_.attr._1)
    }
}
