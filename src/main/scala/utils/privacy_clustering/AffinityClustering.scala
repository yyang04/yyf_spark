package utils.privacy_clustering

import com.linkedin.nn.algorithm.L2ScalarRandomProjectionNNS
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.graphx._
import PrivacyClustering.{privacy_filter, cluster_centers}

import scala.util.control.Breaks

case class Neighbor (vertexId: Long, weight: Double)
case class VertexAttr (parent: Long, neighbor: Neighbor)
case class EdgeAttr (weight: Double)


class AffinityClustering (val upperBound: Int,
                          val lowerBound: Int,
                          val threshold: Int,
                          val numHashes: Int,
                          val signatureLength: Int,
                          val joinParallelism: Int,
                          val bucketLimit: Int,
                          val bucketWidth: Int,
                          val outputPartitions: Int,
                          val num_neighbors: Int,
                          val num_steps: Int
                         )
{
    def fit(sc:SparkContext, x: RDD[(String, Array[Double])]): RDD[(String, Array[Double], Array[Double])] = {

        val numericIDtoStringID = x.zipWithIndex().map(_.swap).persist(StorageLevel.MEMORY_AND_DISK)

        val data = numericIDtoStringID.map{ case(id, (_, emb)) => (id, emb) }

        val edges = create_l2_similarity_graph(data, num_neighbors).map{ case(src, dst, weight) => Edge(src, dst, EdgeAttr(weight)) }.cache()

        val vertex = edges.flatMap{ case Edge(src, dst, w) => List(src, dst) }.distinct().map(x => (x, VertexAttr(x, Neighbor(x, 0.0))))

        val graph = Graph(vertex, edges).cache()

        edges.unpersist()

        var label = cluster(sc, graph)

        var centers = cluster_centers(data, label)

        label = predict(data, centers)

        label = privacy_filter(label, threshold=threshold)

        centers = cluster_centers(data, label)

        val cl = label.join(centers).map{ case(id, (_, center)) => (id, center) }

        val result = numericIDtoStringID.join(cl).map{ case(_, ((uuid, emb), center)) => (uuid, emb, center) }

        numericIDtoStringID.unpersist()

        result
    }


    def create_l2_similarity_graph(x: RDD[(Long, Array[Double])],
                                num_neighbors: Int): RDD[(Long, Long, Double)] = {
        val items = x.map{ case(id, embedding) => (id, Vectors.dense(embedding)) }
        val numFeatures = items.first._2.size
        val numCandidates = num_neighbors
        val model =
            new L2ScalarRandomProjectionNNS()
              .setNumHashes(numHashes)
              .setSignatureLength(signatureLength)
              .setJoinParallelism(outputPartitions)
              .setBucketLimit(bucketLimit)
              .setBucketWidth(bucketWidth)
              .setShouldSampleBuckets(true)
              .setNumOutputPartitions(outputPartitions)
              .createModel(numFeatures)
        val neighbors: RDD[(Long, Long, Double)] = model.getSelfAllNearestNeighbors(items, numCandidates)
        neighbors.map(x=> (x._1, x._2, x._3))
    }

    def predict(x: RDD[(Long, Array[Double])], centers: RDD[(Long, Array[Double])]): RDD[(Long, Int)] = {
        val items = x.map{ case(id, embedding) => (id, Vectors.dense(embedding)) }
        val candidatePool = centers.map{ case (id, embedding) => (id, Vectors.dense(embedding)) }

        val numFeatures = items.first._2.size
        val numCandidates = 1
        val model =
            new L2ScalarRandomProjectionNNS()
              .setNumHashes(numHashes)
              .setSignatureLength(signatureLength)
              .setJoinParallelism(outputPartitions)
              .setBucketLimit(bucketLimit)
              .setBucketWidth(bucketWidth)
              .setShouldSampleBuckets(true)
              .setNumOutputPartitions(outputPartitions)
              .createModel(numFeatures)
        val neighbors: RDD[(Long, Long, Double)] = model.getAllNearestNeighbors(items, candidatePool, numCandidates)
        neighbors.map(x=> (x._1, x._2.toInt))
    }

    def cluster(sc:SparkContext, g:Graph[VertexAttr, EdgeAttr]): RDD[(Long, Int)] ={
        var mst = sc.emptyRDD[Edge[Int]]
        var graph = g

        Breaks.breakable {
            for ( _ <- 0 until num_steps) {
                graph = graph.joinVertices(
                    graph.aggregateMessages[Neighbor](
                        sendMsg = ctx =>
                            if (ctx.dstAttr.parent != ctx.srcAttr.parent) {
                                ctx.sendToSrc(Neighbor(ctx.dstId, ctx.attr.weight))
                                ctx.sendToDst(Neighbor(ctx.srcId, ctx.attr.weight))
                            },
                        mergeMsg = {
                            case (n1, n2) => if (n1.weight > n2.weight) n2 else n1
                        })
                )((_, attr1, attr2) => VertexAttr(attr1.parent, attr2))

                mst = mst.union(
                    Graph(
                        vertices = graph.vertices,
                        edges = graph.vertices.map { case (vid, VertexAttr(parent, _) ) => Edge(vid, parent, 0.0) }
                    ).aggregateMessages[Neighbor](
                        sendMsg = ctx => ctx.sendToDst(ctx.srcAttr.neighbor),
                        mergeMsg = {
                            case (n1, n2) => if (n1.weight > n2.weight) n2 else n1
                        }
                    ).map { case (vid, n) => Edge(vid, n.vertexId, 0.0) })

                mst.foreach(x => println(x))

                val v = mst.flatMap{ case Edge(src, dst, w) => List(src, dst) }.map(x => (x, 0))

                val new_graph = graph.joinVertices(
                    Graph(
                        vertices = v,
                        edges = mst
                    ).connectedComponents.vertices)((_, attr1, attr2) => VertexAttr(attr2, attr1.neighbor)).cache()

                val count = graph.vertices.map{ case(_, attr) => (attr.parent, 1) }.reduceByKey(_ + _).collect().map(_._2)
                if (count.exists(_ > upperBound)) Breaks.break()
                graph = new_graph
                new_graph.unpersist()
            }
        }

        val label = graph.vertices.map{ case(vid, attr) => (vid, attr.parent.toInt) }.cache()
        privacy_filter(label, lowerBound).filter(_._2 != -1)
    }
}
