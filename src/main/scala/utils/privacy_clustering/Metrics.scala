package utils.privacy_clustering
import org.apache.spark.rdd.RDD
import utils.ArrayOperations.{distance, cosineSimilarity}


object Metrics {

    def evaluate_minkowski(x: RDD[(Array[Double], Array[Double])]): Double = {
        val r = x.map {
            case (origin, center) =>
                val l2 = distance(origin, center)
                (l2, 1.0)
        }.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        r._1 / r._2
    }

    def evaluate_cosine(x: RDD[(Array[Double], Array[Double])]): Double = {
        val r = x.map {
            case (origin, center) =>
                val l2 = cosineSimilarity(origin, center)
                (l2, 1.0)
        }.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        r._1 / r._2
    }
}
