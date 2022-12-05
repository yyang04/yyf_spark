package job.remote.flashbuy.U2I

import com.github.jelmerk.knn.scalalike.{Item, floatInnerProduct}
import com.github.jelmerk.knn.scalalike.hnsw.HnswIndex
import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.rdd.RDD
import utils.SparkJobs.RemoteSparkJob
import utils.{ArrayOperations, FileOperations}


case class SkuInfo(id: String, vector: Array[Float]) extends Item[String, Array[Float]] {
    override def dimensions: Int = vector.length
}
case class UserInfo(id: String, vector: Array[Float]) extends Item[String, Array[Float]] {
    override def dimensions: Int = vector.length
}

object U2IInfer extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.beginDt
        val threshold = params.threshold  // 几个邻居
        val threshold2 = params.threshold2  // 几家店
        val ts = params.timestamp  // 模型的ts
        println(s"dt=${dt}")
        println(s"threshold=${threshold}")
        println(s"threshold2=${threshold2}")
        println(s"ts=${ts}")

        val user_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$ts/user_embedding/$dt"
        val sku_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$ts/sku_embedding/$dt"

        // params
        val user = read_raw(sc, user_path)
        val sku = read_raw(sc, sku_path)

        val poi_sku = spark.sql(
            s"""
               |select cast(sku_id as string) as sku_id,
               |       poi_id
               |  from mart_waimaiad.recsys_linshou_pt_poi_skus
               | where dt='$dt'
               |   and city_id in (110100)
               |""".stripMargin).rdd.map { row =>
            val sku_id = row.getAs[String](0)
            val poi_id = row.getAs[Long](1)
            (sku_id, poi_id)
        }.join(sku).map { case (sku, (poi, emb)) => (poi, SkuInfo(sku, emb)) }.groupByKey

        val poi_user = spark.sql(
            s"""
               |select uuid, pois
               |  from mart_waimaiad.recsys_linshou_user_vecs
               | where dt='$dt'
               |   and city_id in (110100)
               |   and size(pois) != 0
               |""".stripMargin).rdd.map { row =>
            val uuid = row.getAs[String](0)
            val pois = row.getAs[Seq[Long]](1)
            (uuid, pois)
        }.join(user).flatMap { case (uuid, (pois, emb)) => pois.map(poi => (poi, UserInfo(uuid, emb))) }.groupByKey

        val res = poi_sku.join(poi_user).flatMap {
            case (poi, (skus, users)) =>
                val index = HnswIndex[String, Array[Float], SkuInfo, Float](32, floatInnerProduct, maxItemCount = skus.size, m = 48, ef = 200, efConstruction = 200)
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
            val scores = ArrayOperations.maxScale(tmp.map(_._2))
            tmp.map(_._1).zip(scores).map { case (sku_id, score) => s"$sku_id:${"%.5f".format(score)}" }
        }.toDF("key", "value")


        val partition = Map("date" -> dt, "branch" -> "u2i", "method" -> "dual_tower_v1")
        FileOperations.saveAsTable(spark, res, "recsys_linshou_multi_recall_results_v2", partition)
    }

    def read_raw(sc: SparkContext, path: String): RDD[(String, Array[Float])] = {
        sc.textFile(path).map { row =>
            val id = row.split(",")(0)
            val emb = row.split(",").drop(1).map(_.toFloat)
            (id, emb)
        }
    }
}

