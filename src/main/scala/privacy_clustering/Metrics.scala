package privacy_clustering

import org.apache.spark.rdd.RDD

case class points(id: String, origin: Array[Double], center: Array[Double])

object Metrics {

    // evaluation_l2
    def evaluate_distance(x: RDD[points]): Double = {
        val r = x.map {
            case points(_, origin, center) =>
                val l2 = distance(origin, center)
                (l2, 1.0)
        }.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        r._1 / r._2
    }

    // L2 distance
    def distance(x: Array[Double], y: Array[Double]): Double = {
        magnitude((x, y).zipped.map(_ - _))
    }

    // evaluation_cosine
    def evaluate_cosine(x: RDD[points]): Double = {
        val r = x.map {
            case points(_, origin, center) =>
                val l2 = cosineSimilarity(origin, center)
                (l2, 1.0)
        }.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        r._1 / r._2
    }

    // cosine 相似度
    def cosineSimilarity(x: Array[Double], y: Array[Double]): Double = {
        require(x.length == y.length)
        dotProduct(x, y) / (magnitude(x) * magnitude(y))
    }

    // 点积
    def dotProduct(x: Array[Double], y: Array[Double]): Double = {
        (for ((a, b) <- x zip y) yield a * b).sum
    }

    // L2 norm
    def magnitude(x: Array[Double]): Double = {
        math.sqrt(x.map(i => i * i).sum)
    }

    // Array 加法
    def add(x: Array[Double], y:Array[Double]): Array[Double] ={
        require(x.length == y.length)
        (x, y).zipped.map(_+_)
    }

    def div(x: Array[Double], y: Double): Array[Double] ={
        x.map(_/y)
    }
}
