package job.remote.flashbuy.u2i

import com.github.jelmerk.knn.scalalike.bruteforce.BruteForceIndex
import com.github.jelmerk.knn.scalalike.floatInnerProduct
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.FileOperations
import utils.SparkJobs.RemoteSparkJob


object EvaluationOffline_v2 extends RemoteSparkJob {

    override def run(): Unit = {

        val dt = params.dt                // 需要evaluate的日期，需要确认模型下面有embedding
        val ts = params.timestamp         // 模型的timestamp
        val threshold = params.threshold  // 对于每个poi请求召回的数量@K

        val user_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$ts/user_embedding/$dt"
        val sku_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$ts/sku_embedding/$dt"
        if (!FileOperations.waitUntilFileExist(hdfs, user_path)) { sc.stop(); return }
        if (!FileOperations.waitUntilFileExist(hdfs, sku_path)) { sc.stop(); return }
        val user_emb = read_raw(sc, user_path)
        val sku_emb = read_raw(sc, sku_path)
        val dim = user_emb.take(1)(0)._2.length

        // 1. 从mv表里选取用户点击广告
        val mv = spark.sql(
            s"""
               | select uuid,
               |        poi_id,
               |        split(reserves["spuIdList"], ",") as spuIdList
               |   from mart_waimai_dw_ad.fact_flow_ad_entry_mv mv
               |   join (select dt,
               |                wm_poi_id
               |  		   from mart_waimai.aggr_poi_info_dd
               | 	      where dt = '$dt' and primary_first_tag_id in (10000000,11000000,5018,12000000,13000000,40000000,41000000,15000000,42000000,5007,5001,1001,22)) info
               |      on mv.dt=info.dt and mv.poi_id=info.wm_poi_id
               |   where mv.dt = '$dt' and is_valid = 'PASS'
               |     and uuid is not null
               |     and split(reserves["spuIdList"], ",") is not null
               |   	 and slot=201
               |     and act=2
               |""".stripMargin).rdd.flatMap { row =>
            val uuid = row.getString(0)
            val poi_id = row.getAs[Long](1)
            val spuIdList = row.getAs[Seq[String]](2).toArray.map(_.toLong)
            spuIdList.map(spuId => (spuId, (uuid, poi_id)))
        }

        // 2. 从铂金表里将spuList 转换为 skuList
        val spu_sku_map = spark.sql(
            s"""
               |select i.sku_id, product_spu_id
               |from mart_waimaiad.recsys_linshou_pt_poi_skus i
               |  join ( select sku_id
               |           from mart_waimaiad.recsys_linshou_pt_poi_skus_high_quality
               |          where dt='$dt' ) h
               |    on i.sku_id=h.sku_id
               |where dt='$dt'
               |""".stripMargin
        ).rdd.map { row =>
            val sku_id = row.getAs[Long](0)
            val spu_id = row.getAs[Long](1)
            (spu_id, sku_id) }
        val poi_uuid_sku = mv.join(spu_sku_map).values.map{ case ((uuid, poi_id), sku_id) => (poi_id, uuid, sku_id) }.cache

        // 3. 按照poi_id进行用户分片
        val poi_user = poi_uuid_sku.map{ case (p, u, s) => (u, p) }
          .distinct
          .join(user_emb)
          .map{ case (u, (p, emb)) => (p, UserInfo(u, emb)) }
          .groupByKey

        // 4. 按照poi_id对sku进行分片
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
                val index = BruteForceIndex[String, Array[Float], SkuInfo, Float](dim, floatInnerProduct)
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
                val sku_predict_unique = sku_predict.distinct
                val sku_real_unique = sku_real.distinct
                val inter = sku_predict_unique.intersect(sku_real_unique)
                val precision_rate = inter.length / sku_real_unique.length
                val recall_rate = inter.length / sku_predict_unique.length
                val count = 1
                (precision_rate, recall_rate, count)
        }.map(_._2).reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
        println(s"precision_rate: ${result._1/result._3}")
        println(s"recall_rate: ${result._2/result._3}")
    }

    def read_raw(sc: SparkContext, path: String): RDD[(String, Array[Float])] = {
        sc.textFile(path).map { row =>
            val id = row.split(",")(0)
            val emb = row.split(",").drop(1).map(_.toFloat)
            (id, emb)
        }
    }
}
