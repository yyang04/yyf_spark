package job.remote.flashbuy.u2i

import com.github.jelmerk.knn.scalalike.floatInnerProduct
import com.github.jelmerk.knn.scalalike.bruteforce.BruteForceIndex
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.{ArrayOperations, FileOperations, MapOperations, RecallEvaluation}
import utils.SparkJobs.RemoteSparkJob


object EvaluationOffline extends RemoteSparkJob {

    override def run(): Unit = {

        val dt = params.dt                // 需要 evaluate 的日期，需要确认模型下面有embedding
        val ts = params.timestamp         // 模型的timestamp
        val threshold = params.threshold  // 对于每个poi请求召回的数量@K
        val user_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$ts/user_embedding/$dt"
        val sku_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$ts/sku_embedding/$dt"
        if (!FileOperations.waitUntilFileExist(hdfs, user_path)) { sc.stop(); return }
        if (!FileOperations.waitUntilFileExist(hdfs, sku_path)) { sc.stop(); return }
        val user_emb = read_raw(sc, user_path)
        val sku_emb = read_raw(sc, sku_path)
        val dim = user_emb.take(1)(0)._2.length

        // 1. 选取用户点击的sku和广告点击的重合的部分
        val poi_uuid_sku = spark.sql(
            s"""
               | select distinct uuid, poi_id, a.sku_id
               |   from mart_lingshou.fact_flow_sdk_product_mv a
               |   join (
               |    select distinct sku_id
               |      from mart_waimaiad.recsys_linshou_pt_poi_skus_high_quality
               |     where dt='$dt') b
               |    on a.sku_id = b.sku_id
               |  where dt='$dt'
               |    and uuid is not null
               |    and a.sku_id is not null
               |    AND event_id in ('b_xU9Ua', 'b_lR1gR')
               |""".stripMargin).rdd.map{ row =>
            val uuid = row.getString(0)
            val poi_id = row.getString(1).toLong
            val sku_id = row.getLong(2).toString
            (poi_id, uuid, sku_id)
        }.distinct.cache()

        // 2. 正样本按照 poi_id 进行分片
        val poi_user = poi_uuid_sku.map{ case (p, u, s) => (u, p) }
          .distinct
          .join(user_emb)
          .map{ case (u, (p, emb)) => (p, UserInfo(u, emb)) }
          .groupByKey

        // 3. 按照 poi_id 选出sku集合
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
        val tmp = poi_sku.join(poi_user).flatMap {
            case (poi, (skus, users)) =>
                val index = BruteForceIndex[String, Array[Float], SkuInfo, Float](dim, floatInnerProduct)
                index.addAll(skus)
                users.par.map { user =>
                    val skuArray = index
                      .findNearest(user.vector, threshold)
                      .map(re => (re.item.id, (1 - re.distance).toDouble))
                      .toArray
                    (poi, user.id, skuArray)
                }.toList
        }.cache

        val coverage = tmp.map(x => (x._1, x._3.map(_._1).groupBy(identity).mapValues(_.length)))
          .reduceByKey(MapOperations.mergeMap)
          .values.map(x => (ArrayOperations.entropy(x.map(_._2.toDouble).toArray), 1d))
          .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

        println(f"Coverage@20: ${coverage._1/coverage._2}%.2f")

        val result = tmp.map(x => (x._2, x._3))
          .groupByKey.mapValues{ iter => iter.flatten.toList.sortBy(_._2).reverse.map(_._1) }
          .join(uuid_sku_real).values.map{
            case (sku_predict, sku_real) =>
                val rel = sku_real.map(_.toLong).toSet
                val predict = sku_predict.map(_.toLong).toArray
                new RecallEvaluation(rel, predict, Array(5, 10, 15, 20))
        }.reduce(_.union(_))
        result.print_entity()
    }

    def read_raw(sc: SparkContext, path: String): RDD[(String, Array[Float])] = {
        sc.textFile(path).map { row =>
            val id = row.split(",")(0)
            val emb = row.split(",").drop(1).map(_.toFloat)
            (id, emb)
        }
    }
}
