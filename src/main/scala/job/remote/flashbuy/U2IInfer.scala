package job.remote.flashbuy
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.tensorflow.SavedModelBundle

import org.{tensorflow => tf}
import utils.FileOperations.saveAsTable
import utils.Murmurhash.hashString
import utils.SparkJobs.RemoteSparkJob
import utils.TimeOperations.getDateDelta


object U2IInfer extends RemoteSparkJob {

    override def run(): Unit = {
        val mode = params.mode
        val dt = params.dt
        val model_path = params.model_path

        val bundle = tf.SavedModelBundle.load(model_path, "serve")
        val broads = sc.broadcast(bundle)
        if (mode.split(",") contains "uuid" ){
            val df = fetch_user_embedding(dt, broads).toDF("key", "embedding")
            saveAsTable(spark, df, "pt_multirecall_u2i_embedding", Map("dt"->dt, "part"->"uuid"))
        }

        if (mode.split(",") contains "sku") {
            val df = fetch_sku_embedding(dt, broads).toDF("key", "embedding")
            saveAsTable(spark, df, "pt_multirecall_u2i_embedding", Map("dt" -> dt, "part" -> "sku_id"))
        }
        bundle.close()
    }


    def fetch_user_embedding(dt:String, broads: Broadcast[SavedModelBundle]): RDD[(String, Array[Float])] = {

        val uuid_embedding = spark.sql(
            s"""
               |select distinct uuid
               |from mart_waimaiad.lingshou_pt_ka_behavior_details_v1
               |where dt between ${getDateDelta(dt, -14)} and $dt
               |  and city_id in (110100)
               |""".stripMargin).rdd.map { row =>
            val uuid = row.getAs[String](0)
            (uuid, hashString("uuid_", uuid))
        }.mapPartitions{ iter =>
            val arr = iter.toArray
            val tmp = arr.map(x => Array(x._2, 0L))
            val sess = broads.value.session()
            val x = tf.Tensor.create(tmp)
            val y = sess.runner()
              .feed("cat_feature:0", x)
              .fetch("user_embedding:0").run().get(0)

            val result = Array.ofDim[Float](y.shape()(0).toInt, y.shape()(1).toInt)
            y.copyTo(result)
            val res = arr.map(_._1).zip(result)
            res.iterator
        }
        uuid_embedding
    }

    def fetch_sku_embedding(dt: String, broads: Broadcast[SavedModelBundle]): RDD[(String, Array[Float])] = {

        val sku_embedding = spark.sql(
            s"""
               |select distinct sku_id
               |from mart_waimaiad.lingshou_pt_ka_behavior_details_v1
               |where dt=$dt
               |""".stripMargin).rdd.map { row =>
            val sku_id = row.getAs[String](0)
            (sku_id, hashString("sku_id_", sku_id))
        }.mapPartitions { iter =>
            val arr = iter.toArray
            val tmp = arr.map(x => Array(0L, x._2))
            val sess = broads.value.session()
            val x = tf.Tensor.create(tmp)
            val y = sess.runner()
              .feed("cat_feature:0", x)
              .fetch("sku_embedding:0").run().get(0)
            val result = Array.ofDim[Float](y.shape()(0).toInt, y.shape()(1).toInt)
            y.copyTo(result)
            val res = arr.map(_._1).zip(result)
            res.iterator
        }
        sku_embedding
    }
}
