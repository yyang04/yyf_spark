package waimai.job.remote.flashbuy.recall.v2i

import com.github.jelmerk.knn.scalalike.bruteforce.BruteForceIndex
import com.github.jelmerk.knn.scalalike.floatInnerProduct
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import waimai.job.remote.flashbuy.recall.v2i.postprocessing.{SkuInfo, UserInfo}
import waimai.utils.{ArrayOp, FileOp}
import waimai.utils.SparkJobs.RemoteSparkJob


object EvaluationCase extends RemoteSparkJob {
    override def run(): Unit = {

        val dt = params.dt
        val threshold = params.threshold    // 每家店推几个
        val threshold2 = params.threshold2  // 几家店
        val ts = params.timestamp           // 模型的 timestamp 保证下面有 user_embedding 和 sku_embedding
        val method = params.algorithm       // 模型最后存的表的 partition

        val user_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$ts/user_embedding/$dt"
        val sku_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$ts/sku_embedding/$dt"
        if (!FileOp.waitUntilFileExist(user_path)) { sc.stop(); return }
        if (!FileOp.waitUntilFileExist(sku_path)) { sc.stop(); return }

        val user = read_raw(sc, user_path)
        val sku = read_raw(sc, sku_path)
        val dim = user.take(1)(0)._2.length

        val poi_sku = spark.sql(
            s"""
               |select distinct sku_id, poi_id
               |  from mart_waimaiad.recsys_linshou_pt_poi_skus_high_quality
               | where dt='$dt'
               |""".stripMargin).rdd.map { row =>
            val sku_id = row.getAs[Long](0).toString
            val poi_id = row.getAs[Long](1)
            (sku_id, poi_id)
        }.join(sku).map { case (sku, (poi, emb)) => (poi, SkuInfo(sku, emb)) }.groupByKey

        val poi_user = spark.sql(
            s"""
               |select uuid, pois
               |  from mart_waimaiad.recsys_linshou_user_vecs
               | where dt='$dt' and size(pois) != 0
               |""".stripMargin).rdd.map { row =>
            val uuid = row.getAs[String](0)
            val pois = row.getAs[Seq[Long]](1)
            (uuid, pois)
        }.join(user).flatMap { case (uuid, (pois, emb)) => pois.map(poi => (poi, UserInfo(uuid, emb))) }.groupByKey

        val res = poi_sku.join(poi_user).flatMap {
            case (poi, (skus, users)) =>
                val index = BruteForceIndex[String, Array[Float], SkuInfo, Float](dim, floatInnerProduct)
                index.addAll(skus)
                users.par.map { user =>
                    val vector = index.findNearest(user.vector, threshold)
                      .map(re => (re.item().id, (1 - re.distance()).toDouble)).toArray
                    (user.id, (poi, vector, vector.map(_._2).sum / vector.length))
                }.toList
        }.groupByKey.mapValues { iter =>
            val result = iter.toArray.sortBy(_._3).reverse.take(threshold2)
            val tmp = result.flatMap {
                case (poi_id, skuIdList, average) => skuIdList
            }
            val scores = tmp.map(_._2)
            tmp.map(_._1).zip(scores).map { case (sku_id, score) => s"$sku_id:${"%.5f".format(score)}" }
        }.toDF("key", "value")
        val partition = Map("date" -> dt, "method" -> method)
        FileOp.saveAsTable(res, "recsys_linshou_multi_recall_results_vtest", partition)
    }

    def read_raw(sc: SparkContext, path: String): RDD[(String, Array[Float])] = {
        sc.textFile(path).map { row =>
            val id = row.split(",")(0)
            val emb = row.split(",").drop(1).map(_.toFloat)
            (id, emb)
        }
    }
}

