package job.remote.flashbuy.U2I

import org.apache.spark.SparkFiles
import utils.SparkJobs.RemoteSparkJob
import org.{tensorflow => tf}
import utils.FileOperations
import utils.Murmurhash.hashString

object SkuEmbGen extends RemoteSparkJob {
    override def run(): Unit = {
        // params
        val dt = params.dt
        val model_path = params.model_path
        val city = params.city match {
            case "" => ""
            case x => s"and city_id in ($x)"
        }

        // broadcast TF Model
        sc.addFile(model_path, recursive = true)
        val bundle = tf.SavedModelBundle.load(SparkFiles.get("tfModel"), "serve")
        val broads = sc.broadcast(bundle)

        // select
        val sku_embedding = spark.sql(
            s"""
               |select sku_id, poi_id, first_category_id, second_category_id, third_category_id, aor_id
               |  from mart_waimaiad.recsys_linshou_pt_poi_skus
               | where dt=$dt $city
               |""".stripMargin).rdd.map { row =>
            val sku_id = row.getAs[Long](0)
            val poi_id = row.getAs[Long](1)
            val hash_sku_id = hashString("sku_id_", sku_id)
            val hash_first_category_id = hashString("first_tag_id_", row.getAs[Long](2))
            val hash_second_category_id = hashString("second_tag_id_", row.getAs[Long](3))
            val hash_third_category_id = hashString("third_tag_id_", row.getAs[Long](4))
            val hash_aor_id = hashString("aor_id_", row.getAs[Long](5))
            val feature = Array(hash_sku_id, hash_first_category_id, hash_second_category_id, hash_third_category_id, hash_aor_id)
            ((sku_id, poi_id), feature)
        }.repartition(2000).mapPartitions { iter =>
            val arr = iter.toArray
            val cat = tf.Tensor.create(arr.map(x => Array(0L) ++ x._2))
            val sess = broads.value.session()
            val y = sess.runner().feed("cat_feature:0", cat).fetch("sku_embedding:0").run().get(0)
            val result = Array.ofDim[Float](y.shape()(0).toInt, y.shape()(1).toInt)
            y.copyTo(result)
            arr.map(_._1).zip(result).map{ case ((sku_id, poi_id), feature_vector) => (sku_id, poi_id, feature_vector)}.iterator
        }.toDF("sku_id, poi_id, feature_vector")
        FileOperations.saveAsTable(spark, sku_embedding, "mart_waimaiad.sg_multirecall_sku_emb", Map("dt" -> dt))
    }
}
