package privacy_clustering

import com.linkedin.nn.algorithm.{CosineSignRandomProjectionNNS, L2ScalarRandomProjectionNNS}
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import privacy_clustering.PrivacyClustering.{cluster_centers, privacy_filter}
import utils.{MST, SparkJob}


class AffinityClustering(val num_neighbors: Int,
                         val upperBounds: Int,
                         val lowerBounds: Int,
                         val threshold: Int,
                         val metric: String,
                         val num_steps_lloyd: Int){

    def create_cosine_similarity_graph(x: RDD[(Int, Array[Double])],
                                num_neighbors: Int): RDD[(Int, Int, Float)] = {
        // create graphs
        val items = x.map { case (id, embedding) => (id.toLong, Vectors.dense(embedding)) }
        val numFeatures = items.first._2.size
        val model = new CosineSignRandomProjectionNNS()
          .setNumHashes(300)
          .setSignatureLength(40)
          .setJoinParallelism(4000)
          .setBucketLimit(1000)
          .setShouldSampleBuckets(true)
          .setNumOutputPartitions(8000)
          .createModel(numFeatures)
        val edges: RDD[(Long, Long, Double)] = model.getSelfAllNearestNeighbors(items, num_neighbors)
        edges.map { case (src, dst, weight) => (src.toInt, dst.toInt, weight.toFloat) }
    }

    def create_l2_similarity_graph(x: RDD[(Int, Array[Double])],
                                   num_neighbors: Int): RDD[(Int, Int, Float)] = {
        val items = x.map { case (id, embedding) => (id.toLong, Vectors.dense(embedding)) }
        val numFeatures = items.first._2.size
        val model = new L2ScalarRandomProjectionNNS()
          .setNumHashes(300)
          .setSignatureLength(40)
          .setJoinParallelism(8000)
          .setBucketLimit(1000)
          .setBucketWidth(2)
          .setShouldSampleBuckets(true)
          .setNumOutputPartitions(8000)
          .createModel(numFeatures)
        val edges: RDD[(Long, Long, Double)] = model.getSelfAllNearestNeighbors(items, num_neighbors)
        edges.map { case (src, dst, weight) => (src.toInt, dst.toInt, weight.toFloat) }
    }

    // predict
    def predict(x: RDD[(Int, Array[Double])],
                Center: RDD[(Int, Array[Double])]
               ): RDD[(Int, Int)] = {
        val items = Center.zipWithIndex().map { case ((id, embedding), centerID) => (centerID, Vectors.dense(embedding)) }.cache()
        val queries = x.map { case (id, embedding) => (id.toLong, Vectors.dense(embedding)) }

        val numFeatures = items.first._2.size
        val model = new L2ScalarRandomProjectionNNS()
          .setNumHashes(300)
          .setSignatureLength(40)
          .setJoinParallelism(8000)
          .setBucketLimit(1000)
          .setBucketWidth(2)
          .setShouldSampleBuckets(true)
          .setNumOutputPartitions(8000)
          .createModel(numFeatures)

        val res: RDD[(Int, Int)] = model.getAllNearestNeighbors(queries, items, k=1).map{ case(queryID, centerID, weight) => (queryID.toInt, centerID.toInt) }
        items.unpersist()
        res
    }


    def fit(x: RDD[(String, Array[Double])], sc: SparkContext): RDD[(String, Array[Double], Array[Double])] = {
        val tmp = x.zipWithIndex().map{ case(entity, id) => (entity, id.toInt) }.cache()
        val intID2StringID = tmp.map{ case ((uuid, emb), id) => (id, (uuid, emb))}
        val data = tmp.map{ case((uuid, emb), id) => (id, emb) }
        // sample
        val sample = data.sample(withReplacement = false, fraction = 0.1).cache()

        // create graphs
        val edges = create_l2_similarity_graph(sample, num_neighbors).cache()
        sample.unpersist()

        val MSTK = edges.collect()
        edges.unpersist()

        val UF = MST.MSTBClustering(MSTK, upperBounds)
        UF.drop_lower_bounds(lowerBounds)

        var labels = sc.parallelize(UF.get_partitions.zipWithIndex.flatMap{
            case (ids, label) => ids.map(id => (id, label)) }, 6000).cache()

        var centers = cluster_centers(sample, labels)  //
        labels = predict(data, centers)

        for (_ <- 0 until num_steps_lloyd) {
            centers = cluster_centers(data, labels)
            labels = predict(data, centers)
        }

        labels = privacy_filter(labels, threshold)
        centers = cluster_centers(data, labels)

        intID2StringID.join(centers).map{ case(_, ((uuid, emb), center)) => (uuid, emb, center)}
    }
}
