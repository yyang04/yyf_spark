package privacy_clustering
import org.apache.spark.rdd.RDD
import breeze.linalg.{*, Axis, norm, DenseMatrix => BDM, DenseVector => BDV}
import org.apache.spark.ml.linalg.{DenseMatrix, Vectors}

import java.util.{Random => JR}
import privacy_clustering.PrivacyClustering.{cluster_centers, privacy_filter}
import utils.SparkJob


class SimHash(val p: Int,
              val seed: Int,
              val threshold: Int) {


    def fit(x: RDD[(String, Array[Double])]): RDD[(String, Array[Double], Array[Double])] = {
        val tmp = x.zipWithIndex().map{ case(entity, id) => (entity, id.toInt) }.cache()
        var intID2StringID = tmp.map{ case ((uuid, emb), id) => (id, (uuid, emb))}
        val data = tmp.map{ case((uuid, emb), id) => (id, emb) }

        // get dimension
        val dim = data.first._2.length
        // get transformation matrix
        var v = new BDM(dim, p, DenseMatrix.randn(dim, p, new JR(seed)).toArray)
        // normalize vertically
        v = v(*, ::) / norm(v, Axis._0).t
        // matrix multiplication and hash
        val hash_labels = data.map {
            case (id, embed) =>
                val Z = (BDV(embed).t * v).t.toArray.map(x => {
                    if (x > 0) 1 else 0
                })
                val hash_label = Z.map(_.toString).mkString
                (id, hash_label)
        }.cache()

        // hash_label to label
        val m = hash_labels.map(_._2).distinct().zipWithIndex()
          .map{ case(hash_label, label) => (hash_label, label.toInt) }

        // id to cluster label
        var labels = hash_labels.map(_.swap).join(m).map{
            case (hash_label, (id, label)) => (id, label)
        }.cache()

        // group_low_frequency_items
        labels = privacy_filter(labels, threshold=threshold).cache()

        // calculate cluster centers
        val centers = cluster_centers(data, labels)

        // restore id to uuid
        val cl = labels.join(centers).map{ case(id, (label, center)) => (id, center)}

        // return result
        intID2StringID.join(cl).map{ case(_, ((uuid, emb), center)) => (uuid, emb, center)}
    }
}


object SimHashClustering extends SparkJob {
    def main(args: Array[String]): Unit = {





    }
}

