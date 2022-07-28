package utils.PrivacyClustering

import com.linkedin.nn.algorithm.L2ScalarRandomProjectionNNS
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vectors
import PrivacyClustering.{cluster_centers, privacy_filter}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
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
    def fit(sc: SparkContext,
            x: RDD[(String, Array[Double])]
           ): RDD[(String, Array[Double], Array[Double])] = {

        // 重新弄序号
        val numericIDtoStringID = x.zipWithIndex().map(_.swap).cache()

        val data = numericIDtoStringID.map{ case(id, (_, emb)) => (id, emb) }

        // 采样
        val sampleData = data.sample(withReplacement=false, fraction=sample_fraction).cache()

        // 生成图
        val edges = create_l2_similarity_graph(sampleData, num_neighbors).map{ case(src, dst, weight) => (src.toInt, dst.toInt) }

        // 把边直接 collect 到 driver 端
        val local_edges = edges.collect

        // 聚类 + 过滤掉超低频
        val UF = MSTBCompressed(local_edges).drop_lower_bounds(lowerBound)

        // 生成标签
        var labels = sc.parallelize(UF.get_partitions.zipWithIndex.flatMap(x => x._1.map(e => (e.toLong, x._2))), 2000)

        // 生成中心向量
        var centers = cluster_centers(sampleData, labels)

        // 根据中心向量，重新分布所有节点
        labels = predict(data, centers)

        // 一轮迭代
        for (_ <- 0 until 1) {
            centers = cluster_centers(data, labels)
            labels = predict(data, centers)
        }

        // 过滤掉低频节点
        labels = privacy_filter(labels, threshold=threshold)

        // 生成中心向量
        centers = cluster_centers(data, labels)

        // 把id和中心向量对应上
        val cl = labels.join(centers).map{ case(id, (label, center)) => (id, center)}

        // 重新用uuid 作为id
        val result = numericIDtoStringID.join(cl).map{ case(_, ((uuid, emb), center)) => (uuid, emb, center)}

        numericIDtoStringID.unpersist()

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

        // val max_iterations = math.ceil(math.log(upperBounds) / math.log(2)).toInt
        // val max_iterations = 1
//        Breaks.breakable{
//            for (_ <- 0 until max_iterations) {
//                val pre = UF.get_items.length
//                val mst_step = mutable.ListBuffer[(Int, Int, Float)]()
//                val min_edges = mutable.Map[Int, (Int, Int, Float)]()
//
//                for (e <- edges) {
//                    if (!(min_edges contains e._1) || (min_edges(e._1)._3 > e._3)) {
//                        min_edges += e._1 -> e
//                    }
//                    if (!(min_edges contains e._2) || (min_edges(e._2)._3 > e._3)) {
//                        min_edges += e._2 -> (e._2, e._1, e._3)
//                    }
//                }
//                for ((u,v,w) <- min_edges.values){
//                    val u_parent = UF.find(u)
//                    val v_parent = UF.find(v)
//                    val u_size = UF.size(u_parent)
//                    val v_size = UF.size(v_parent)
//
//                    if (u_parent != v_parent && u_size < upperBounds && v_size < upperBounds) {
//                        UF.union(u_parent, v_parent)
//                        mst_step.append((u,v,w))
//                    }
//                }
//                if (pre == UF.get_items.length) Breaks.break()
//                edges = edges -- mst_step.toSet
//                mst.append(mst_step)
//            }
//        }
//        UF
//    }


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
}
