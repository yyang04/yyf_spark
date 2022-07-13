package privacy_clustering

import org.apache.spark.rdd.RDD
import privacy_clustering.Metrics.{add, div}

object PrivacyClustering {
    // data[id, cluster] label need cached
    def privacy_filter(label: RDD[(Int, Int)],
                       threshold: Int): RDD[(Int, Int)]={
        val labelCount = label.map{
            case (_, label) => (label, 1)
        }.reduceByKey(_+_)

        label.map(_.swap).join(labelCount).map{
            case (label, (id, count)) =>
                val nl = if (count >= threshold) label else -1
                (id, nl)
        }
    }

    // label need cached
    def cluster_centers(x: RDD[(Int, Array[Double])],
                        label: RDD[(Int, Int)]
                       ): RDD[(Int, Array[Double])] ={

        val le = x.join(label)
          .map{
            case (_, (embedding, label)) => (label, (embedding, 1))
        }.reduceByKey((x, y) => (add(x._1, y._1), x._2 + y._2))
          .map{
            case (label, (embedding, count)) => (label, div(embedding, count.toDouble))
        }

        label.map(_.swap).join(le).map{ case (_, (id, embedding)) => (id, embedding)}
    }
}
