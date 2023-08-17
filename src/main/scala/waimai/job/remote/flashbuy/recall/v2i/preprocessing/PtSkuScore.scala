package waimai.job.remote.flashbuy.recall.v2i.preprocessing

import waimai.utils.DateOp.{getNDaysAgo, getNDaysAgoFrom}
import waimai.utils.FileOp.saveAsTable
import waimai.utils.SparkJobs.RemoteSparkJob

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

object PtSkuScore extends RemoteSparkJob {
    override def run(): Unit = {
        // 铂金闪购商品点击质量评分
        val tableName = ""
        val dt = params.dt match { case "" => getNDaysAgo(1); case x => x }

        // b_waimai_gu3tgj65_mc
        // b_waimai_x0yyxqjr_mc
        // b_l9TCv


        val ptRaw = spark.sql(
            s"""
               |select reserves['spu_id'] as spu_id,
               |       sku_id
               |  from mart_waimai_dw_ad.fact_flow_ad_entry_mv mv
               |  join (
               |    select poi_id
               |      from mart_lingshou.aggr_poi_info_dd
               |     where dt='$dt'
               |  ) info on mv.poi_id=info.poi_id
               |  join (
               |      select spu_id,
               |             sku_id
               |        from (
               |          select product_spu_id as spu_id,
               |                 sku_id,
               |                 row_number() OVER (PARTITION BY product_spu_id ORDER BY price DESC) AS rank
               |            from mart_waimaiad.recsys_linshou_pt_poi_skus
               |            where dt='$dt'
               |        ) where rank=1
               |  ) spusku on reserves['spu_id']=spusku.spu_id
               |  where mv.dt between '${getNDaysAgoFrom(dt, 30)}' and '$dt'
               |    and mv.slot in (160,161,162,192,193,195)
               |    and mv.is_valid='PASS'
               |    and reserves['spu_id'] is not null
               |""".stripMargin
        )

        val other= spark.sql(
            s"""
               |select mv.sku_id,
               |       info.poi_id,
               |       info.second_category_id,
               |       count(*) as cnt
               |  from mart_lingshou.fact_flow_sdk_product_mv mv
               |  join (
               |    select sku_id,
               |           poi_id,
               |           second_category_id
               |      from mart_waimaiad.recsys_linshou_pt_poi_skus
               |     where dt='$dt'
               |  ) info on mv.sku_id=info.sku_id
               | where dt between '${getNDaysAgoFrom(dt, 30)}' and '$dt'
               |   and event_id in ('b_waimai_gu3tgj65_mc', 'b_waimai_x0yyxqjr_mc', 'b_l9TCv')
               |   and sku_id is not null and sku_id != 0
               |   group by 1,2,3
               |""".stripMargin).rdd.map { row =>
            val sku_id = row.getLong(0)
            val poi_id = row.getLong(1)
            val second_category_id = row.getLong(2)
            val score = row.getLong(3)
            (poi_id, (second_category_id, sku_id, score))
        }.groupByKey.cache()




//
//        val lowN = highQualitySkuTmp.filter( x => x._2.size <= threshold ).mapValues{ _.toList }
//        val topN = highQualitySkuTmp.filter( x => x._2.size > threshold )
//
//        val highQualitySku = topN.mapValues{ iter =>
//            val skuList = ArrayBuffer[(Long, Long, Long)]()
//            val result = iter.toArray.groupBy(_._1).mapValues { iterArray => iterArray.sortBy(_._3).reverse }
//            val loop = new Breaks
//            loop.breakable {
//                var i = 0
//                while (true) {
//                    for (k <- result.values) {
//                        if (skuList.size < threshold) {
//                            try {
//                                skuList.append(k(i))
//                            } catch {
//                                case _: Exception =>
//                            }
//                        } else {
//                            loop.break()
//                        }
//                    }
//                    i = i + 1
//                }
//            }
//            skuList.toList
//        }.union(lowN)
//          .flatMap{ case (poi_id, skuInfoList) => skuInfoList
//          .map{ case (third_category_id, sku_id, score) => (poi_id, third_category_id, sku_id, score)}
//        }.toDF("poi_id", "third_category_id", "sku_id", "score")
//
//        saveAsTable(spark, highQualitySku, "recsys_linshou_pt_poi_skus_high_quality", Map("dt" -> s"$dt"))
    }
}
