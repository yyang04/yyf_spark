package utils.privacy_clustering

import org.apache.spark.rdd.RDD

case class points(id: String, origin: Array[Double], center: Array[Double])

object Metrics {

    // evaluation_l2
//    def evaluate_minkowski(x: RDD[points]): Double = {
//        val r = x.map {
//            case points(_, origin, center) =>
//                val l2 = distance(origin, center)
//                (l2, 1.0)
//        }.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
//        r._1 / r._2
//    }
//
//    // evaluation_cosine
//    def evaluate_cosine(x: RDD[points]): Double = {
//        val r = x.map {
//            case points(_, origin, center) =>
//                val l2 = cosineSimilarity(origin, center)
//                (l2, 1.0)
//        }.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
//        r._1 / r._2
//    }

    // L2 distance









}
