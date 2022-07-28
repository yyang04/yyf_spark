package utils.PrivacyClustering

import breeze.linalg.{*, Axis, norm, DenseMatrix => BDM, DenseVector => BDV}
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils.PrivacyClustering.PrivacyClustering.{cluster_centers, privacy_filter}

import java.util.{Random => JR}
import scala.collection.mutable

class PrefixLshClustering (val p: Int,
                           val seed: Int,
                           val threshold: Int)
{
    def fit(x: RDD[(String, Array[Double])]): RDD[(String, Array[Double], Array[Double])] = {
        val numericIDtoStringID = x.zipWithIndex().map(_.swap).persist(StorageLevel.MEMORY_AND_DISK)

        val data = numericIDtoStringID.map{ case(id, (_, emb)) => (id, emb) }

        // get dimension
        val dim = data.first._2.length

        // get transformation matrix and normalize
        var v = new BDM(dim, p, DenseMatrix.randn(dim, p, new JR(seed)).toArray)
        v = v(*, ::) / norm(v, Axis._0).t

        // matrix multiplication and hash
        var hash_labels = data.map {
            case (id, emb) =>
                val Z = (BDV(emb).t * v).t.toArray.map(x => { if (x > 0) 1 else 0 })
                val hash_label = Z.map(_.toString).mkString
                (id, hash_label)
        }.cache()

        // prefixLSH
        val counter = pre_count(hash_labels)
        val dict = bfs(counter)
        hash_labels = hash_labels.map{ case (id, embedding) => (id, dict(embedding)) }

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

    def pre_count(hash_labels: RDD[(Long, String)]): Map[String, (Int, List[String])] = {
        val counter = new mutable.ListBuffer[(String, (Int, List[String]))]
        var tmp = hash_labels.map(x => (x._2, 1)).reduceByKey(_+_).map{
            case(id, count) => (id, (count, List[String](id)))
        }.collect().toList
        counter ++= tmp
        for (i <- 0 until p) {
            tmp = tmp.groupBy(_._1.substring(0, p - 1 - i)).mapValues(x => {
                (x.map(_._2._1).sum, x.flatMap(_._2._2))
            }).toList
            counter ++= tmp
        }
        counter.toMap
    }

    def bfs(counter:Map[String, (Int, List[String])]): mutable.Map[String, String] = {
        val q = new mutable.Queue[String]
        val res = mutable.Map[String, List[String]]()

        q.enqueue("")
        while (q.nonEmpty) {
            val e = q.dequeue()
            val left_count = if (counter contains(e + "0")) Some(counter(e + "0")._1) else None
            val right_count = if (counter contains(e + "1")) Some(counter(e + "1")._1) else None
            (left_count, right_count) match {
                case (None, None) => res += e -> counter(e)._2
                case (_ , None) => q.enqueue(e + "0")
                case (None, _)  => q.enqueue(e + "1")
                case (Some(x), Some(y)) if x >= this.threshold && y >= this.threshold => q.enqueue(e + "0"); q.enqueue(e + "1")
                case _ => res += e -> counter(e)._2
            }
        }
        res.flatMap{ case(k,v) => v.map((_,k)) }
    }
}
