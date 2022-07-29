package utils.PrivacyClustering

import com.linkedin.nn.algorithm.L2ScalarRandomProjectionNNS
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vectors
import PrivacyClustering.{cluster_centers, privacy_filter}
import utils.AF.ArrayUnionFind


class AffinityClusteringLocal (val upperBound: Int,
                               val lowerBound: Int,
                               val threshold: Int,
                               val numHashes: Int,
                               val signatureLength: Int,
                               val joinParallelism: Int,
                               val bucketLimit: Int,
                               val bucketWidth: Int,
                               val outputPartitions: Int,
                               val num_neighbors: Int,
                               val sample_fraction: Double)
{
    def fit(sc: SparkContext, x: RDD[(String, Array[Double])]): RDD[(String, Array[Double], Array[Double])] = {

        // 重新序号
        val numericIDtoStringID = x.zipWithIndex().map(_.swap).cache()
        // 需要处理的data
        val data = numericIDtoStringID.map{ case(id, (_, emb)) => (id, emb) }
        // 采样过后的data
        val sampleData = data.sample(withReplacement=false, fraction=sample_fraction).cache()
        // 利用采样过后的data生成最近邻图
        val edges = create_l2_similarity_graph(sampleData, num_neighbors).map{ case(src, dst, _) => (src.toInt, dst.toInt) }
        // 把图的edges提取到driver侧
        val local_edges = edges.collect
        // 聚类 + 过滤掉超低频节点
        val UF = MSTBCompressed(local_edges).drop_lower_bounds(lowerBound)
        // 取出每个分片的节点，然后给sample节点打上标签label，分配给executor端
        var labels = sc.parallelize(UF.get_partitions.zipWithIndex.flatMap{ case( nodeList, id ) => nodeList.map(node => (node.toLong, id)) }, 2000)
        // 根据采样节点生成中心向量
        var centers = cluster_centers(sampleData, labels)
        // 根据中心向量，重新分布所有节点
        labels = predict(data, centers)
        // 劳伦兹迭代
        for (_ <- 0 until 1) {
            centers = cluster_centers(data, labels)
            labels = predict(data, centers)
        }
        // 硬性过滤掉低频节点
        labels = privacy_filter(labels, threshold=threshold)
        // 重新生成中心向量
        centers = cluster_centers(data, labels)
        // 把id和中心向量对应上
        val cl = labels.join(centers).map{ case(id, (label, center)) => (id, center)}
        // 重新用uuid 作为id
        val result = numericIDtoStringID.join(cl).map{ case(_, ((uuid, emb), center)) => (uuid, emb, center)}

        numericIDtoStringID.unpersist()
        // 返回结果
        result
    }

    def MSTBCompressed(ed: Array[(Int, Int)]): ArrayUnionFind = {

        val edges = ed.toSet
        val V = edges.toList.flatMap(x => List(x._1, x._2)).toSet
        val UF = new ArrayUnionFind(V)
        for ((u, v) <- edges) {
            val u_parent = UF.find(u)
            val v_parent = UF.find(v)
            if (u_parent != v_parent) {
                val u_size = UF.size(u_parent)
                val v_size = UF.size(v_parent)
                if (u_size < upperBound && v_size < upperBound) {
                    UF.union(u_parent, v_parent)
                }
            }
        }
        UF
    }

    def create_l2_similarity_graph(x: RDD[(Long, Array[Double])],
                                   num_neighbors: Int): RDD[(Long, Long, Double)] = {
        val items = x.map{ case(id, embedding) => (id, Vectors.dense(embedding)) }
        val numFeatures = items.first._2.size
        val numCandidates = num_neighbors
        val model =
            new L2ScalarRandomProjectionNNS()
              .setNumHashes(numHashes)                   // 300
              .setSignatureLength(signatureLength)       // 10
              .setJoinParallelism(outputPartitions)      // 8000
              .setBucketLimit(bucketLimit)               // 2000
              .setBucketWidth(bucketWidth)               // 10
              .setShouldSampleBuckets(true)              // true
              .setNumOutputPartitions(outputPartitions)  // 8000
              .createModel(numFeatures)

        val neighbors: RDD[(Long, Long, Double)] = model.getSelfAllNearestNeighbors(items, numCandidates)  // 5
        neighbors.map(x=> (x._1, x._2, x._3))
    }

    def predict(x: RDD[(Long, Array[Double])], centers: RDD[(Long, Array[Double])]): RDD[(Long, Int)] = {
        val items = x.map{ case(id, embedding) => (id, Vectors.dense(embedding)) }

        val candidatePool = centers.map{ case(_, embedding) => Vectors.dense(embedding) }.distinct.zipWithIndex.map(_.swap)

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
}
