package utils.privacy_clustering

import org.apache.spark.rdd.RDD
import breeze.linalg.{*, Axis, norm, DenseMatrix => BDM, DenseVector => BDV}
import org.apache.spark.ml.linalg.DenseMatrix

import java.util.{Random => JR}
import PrivacyClustering.{cluster_centers, privacy_filter}
import org.apache.spark.storage.StorageLevel


class SimHashClustering(val p: Int,
              val seed: Int,
              val threshold: Int) {


    def fit(x: RDD[(String, Array[Double])]): RDD[(String, Array[Double], Array[Double])] = {
        val numericIDtoStringID = x.zipWithIndex().map(_.swap).persist(StorageLevel.MEMORY_AND_DISK)


        val data = numericIDtoStringID.map{ case(id, (_, emb)) => (id, emb) }

        // get dimension
        val dim = data.first._2.length

        // get transformation matrix and normalize
        var v = new BDM(dim, p, DenseMatrix.randn(dim, p, new JR(seed)).toArray)
        v = v(*, ::) / norm(v, Axis._0).t

        // matrix multiplication and hash
        val hash_labels = data.map {
            case (id, emb) =>
                val Z = (BDV(emb).t * v).t.toArray.map(x => { if (x > 0) 1 else 0 })
                val hash_label = Z.map(_.toString).mkString
                (id, hash_label)
        }.cache()

        // hash_label to label
        val m = hash_labels
          .map(_._2).distinct().zipWithIndex()
          .map{ case(hash_label, label) => (hash_label, label.toInt) }

        // id to cluster label
        var labels = hash_labels.map(_.swap).join(m).map{
            case (hash_label, (id, label)) => (id, label)
        }
        hash_labels.unpersist()

        // group_low_frequency_items
        labels = privacy_filter(labels, threshold=threshold)

        // calculate cluster centers
        val centers = cluster_centers(data, labels)

        // restore id to uuid
        val cl = labels.join(centers).map{ case(id, (label, center)) => (id, center)}

        // return result
        val result = numericIDtoStringID.join(cl).map{ case(_, ((uuid, emb), center)) => (uuid, emb, center)}

        numericIDtoStringID.unpersist()

        result
    }
}
