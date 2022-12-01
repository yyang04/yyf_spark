package job.remote.flashbuy.U2I

import org.apache.spark.SparkFiles
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.tensorflow.SavedModelBundle
import utils.FileOperations.saveAsTable
import utils.Murmurhash.hashString
import utils.SparkJobs.RemoteSparkJob
import utils.TimeOperations.getDateDelta

object U2IInfer extends RemoteSparkJob {

//    override def run(): Unit = {
//        // get params
//        val mode = params.mode
//        val dt = params.dt
//        val model_path = params.model_path
//
//
//        sc.addFile(model_path, true)
//        val path = SparkFiles.get("tfModel")
//        // val bundle = tf.SavedModelBundle.load(path, "serve")
//        val broads = sc.broadcast(bundle)
//
//        if (mode.split(",") contains "uuid") {
//            val df = fetch_user_embedding(dt, broads).toDF("key", "embedding")
//            saveAsTable(spark, df, "pt_multirecall_u2i_embedding", Map("dt" -> dt, "part" -> "uuid"))
//        }
//
//        if (mode.split(",") contains "sku") {
//            val df = fetch_sku_embedding(dt, broads).toDF("key", "embedding")
//            saveAsTable(spark, df, "pt_multirecall_u2i_embedding", Map("dt" -> dt, "part" -> "sku_id"))
//        }
//        bundle.close()
//    }
//
//
//    def fetch_user_embedding(dt: String, broads: Broadcast[SavedModelBundle]): RDD[(String, Array[Float])] = {
//
//        val uuid_embedding = spark.sql(
//            s"""
//               |select distinct uuid
//               |from mart_waimaiad.lingshou_pt_ka_behavior_details_v1
//               |where dt between ${getDateDelta(dt, -14)} and $dt
//               |  and city_id in (110100)
//               |""".stripMargin).rdd.map { row =>
//            val uuid = row.getAs[String](0)
//            (uuid, hashString("uuid_", uuid))
//        }.mapPartitions { iter =>
//            val arr = iter.toArray
//            val tmp = arr.map(x => Array(x._2, 0L))
//            val sess = broads.value.session()
//            val x = tf.Tensor.create(tmp)
//            val y = sess.runner()
//              .feed("cat_feature:0", x)
//              .fetch("user_embedding:0").run().get(0)
//
//            val result = Array.ofDim[Float](y.shape()(0).toInt, y.shape()(1).toInt)
//            y.copyTo(result)
//            val res = arr.map(_._1).zip(result)
//            res.iterator
//        }
//        uuid_embedding
//    }
//
//    def fetch_sku_embedding(dt: String, broads: Broadcast[SavedModelBundle]): RDD[(String, Array[Float])] = {
//
//        val sku_embedding = spark.sql(
//            s"""
//               |select sku_id, poi_id, first_category_id, second_category_id, third_category_id
//               |  from mart_waimaiad.recsys_linshou_pt_poi_skus
//               | where dt=$dt
//               |""".stripMargin).rdd.map { row =>
//            val sku_id = row.getAs[Long](0)
//            val poi_id = row.getAs[Long](1)
//            val first_category_id = row.getAs[Long](2)
//            val second_category_id = row.getAs[Long](3)
//            val third_category_id = row.getAs[Long](4)
//
//            (poi_id, (sku_id, first_category_id, second_category_id, third_category_id))
//            (sku_id, hashString("sku_id_", sku_id.toString))
//        }.mapPartitions { iter =>
//            val arr = iter.toArray
//            val tmp = arr.map(x => Array(0L, x._2))
//            val sess = broads.value.session()
//            val x = tf.Tensor.create(tmp)
//            val y = sess.runner()
//              .feed("cat_feature:0", x)
//              .fetch("sku_embedding:0").run().get(0)
//            val result = Array.ofDim[Float](y.shape()(0).toInt, y.shape()(1).toInt)
//            y.copyTo(result)
//            val res = arr.map(_._1).zip(result)
//            res.iterator
//        }.toDF("sku_id", "poi_id", "feature_vector")
//        sku_embedding
//    }
}
