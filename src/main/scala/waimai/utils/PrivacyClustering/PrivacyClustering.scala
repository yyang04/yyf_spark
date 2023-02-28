package waimai.utils.PrivacyClustering

import org.apache.spark.rdd.RDD
import waimai.utils.ArrayOperations

object PrivacyClustering {
    // data[id, cluster] label need cached
    def privacy_filter(label: RDD[(Long, Int)],
                       threshold: Int): RDD[(Long, Int)] = {
        val label_cached = label.cache()
        val labelCount = label.map{
            case (_, label) => (label, 1)
        }.reduceByKey(_+_)

        val result = label.map(_.swap).join(labelCount).map {
            case (label, (id, count)) =>
                val nl = if (count >= threshold) label else -1
                (id, nl)
        }

        label_cached.unpersist()
        result
    }

    // label need cached
    def cluster_centers(x: RDD[(Long, Array[Double])],
                        label: RDD[(Long, Int)]
                       ): RDD[(Long, Array[Double])] ={

        val label_cached = label.cache()
        val le = x.join(label_cached)
          .map{
            case (_, (embedding, label)) => (label, (embedding, 1))
        }.reduceByKey((x, y) => (ArrayOperations.add(x._1, y._1), x._2 + y._2))
          .map{
            case (label, (embedding, count)) => (label, ArrayOperations.div(embedding, count.toDouble))
        }
        val result = label_cached.map(_.swap).join(le).map{ case (_, (id, embedding)) => (id, embedding) }
        label_cached.unpersist()

        result
    }
}
