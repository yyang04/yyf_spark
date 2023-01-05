package job.remote.flashbuy.u2i

import com.github.jelmerk.knn.scalalike.floatInnerProduct
import com.github.jelmerk.knn.scalalike.bruteforce.BruteForceIndex
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.FileOperations
import utils.SparkJobs.RemoteSparkJob


object EvaluationOffline extends RemoteSparkJob {

    override def run(): Unit = {

        val dt = params.dt   // 需要 evaluate 的日期
        val ts = params.timestamp // 模型的 timestamp
        val threshold = params.threshold  // 对于每个poi请求召回的数量@K

        val user_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$ts/user_embedding/$dt"
        val sku_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$ts/sku_embedding/$dt"

        if (!FileOperations.waitUntilFileExist(hdfs, user_path)) { sc.stop(); return }
        if (!FileOperations.waitUntilFileExist(hdfs, sku_path)) { sc.stop(); return }

        val user_emb = read_raw(sc, user_path)
        val sku_emb = read_raw(sc, sku_path)

        val poi_uuid_sku = spark.sql(
            s"""
               | select distinct uuid, poi_id, sku_id
               |   from mart_waimaiad.sg_pt_click_log_v1
               |  where dt='$dt'
               |    and event_id in ('b_sct3Y', 'b_waimai_leosvgq2_mc')
               |""".stripMargin).rdd.map{ row =>
            val uuid = row.getString(0)
            val poi_id = row.getLong(1)
            val sku_id = row.getString(2)
            (poi_id, uuid, sku_id)
        }.cache()

        val poi_user = poi_uuid_sku.map{ case (p, u, s) => (u, p) }
          .distinct
          .join(user_emb)
          .map{ case (u, (p, emb)) => (p, UserInfo(u, emb)) }
          .groupByKey

        val poi_sku = spark.sql(
            s"""
               |select poi_id, sku_id
               |  from mart_waimaiad.recsys_linshou_pt_poi_skus_high_quality
               | where dt='$dt'
               |""".stripMargin).rdd.map{ row =>
            val poi_id = row.getLong(0)
            val sku_id = row.getLong(1).toString
            (sku_id, poi_id)
        }.join(sku_emb)
          .map{ case (s, (p, emb)) => (p, SkuInfo(s, emb)) }
          .groupByKey

        val uuid_sku_real = poi_uuid_sku.map{ case(p, u, s) => (u, s) }.groupByKey.mapValues(_.toList)
        val result = poi_sku.join(poi_user).flatMap {
            case (poi, (skus, users)) =>
                val index = BruteForceIndex[String, Array[Float], SkuInfo, Float](32, floatInnerProduct)
                index.addAll(skus)
                users.par.map { user =>
                    val skuArray = index
                      .findNearest(user.vector, threshold)
                      .map(re => (re.item.id, (1 - re.distance).toDouble))
                      .toArray
                    (user.id, skuArray)
                }.toList
        }.groupByKey.mapValues{ iter => iter.flatten.toList.map(_._1) }
          .join(uuid_sku_real).mapValues{
            case (sku_predict, sku_real) =>
                val inter_length = sku_predict.intersect(sku_real).length.toDouble
                val real_length = sku_real.distinct.length.toDouble
                val recall_rate = inter_length / real_length
                val count = 1
                (inter_length, real_length, recall_rate, count)
        }.map(_._2).reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))

        println(s"inter_length: ${result._1}")
        println(s"full_length: ${result._2}")
        println(s"recall_rate: ${result._3/result._4}")
        println(s"count: ${result._4}")
    }

    def read_raw(sc: SparkContext, path: String): RDD[(String, Array[Float])] = {
        sc.textFile(path).map { row =>
            val id = row.split(",")(0)
            val emb = row.split(",").drop(1).map(_.toFloat)
            (id, emb)
        }
    }
}
