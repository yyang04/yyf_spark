package waimai.job.remote.flashbuy.u2i

import waimai.utils.FileOperations.saveAsTable
import waimai.utils.TimeOperations.getDateDelta
import waimai.utils.SparkJobs.RemoteSparkJob

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

object HighQualitySku extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.dt
        val threshold = params.threshold

        val highQualitySkuTmp = spark.sql(
            s"""
               |      select info.sku_id,
               |             poi_id,
               |             third_category_id,
               |             coalesce(mv.score, 0) as score
               |      from mart_waimaiad.recsys_linshou_pt_poi_skus info
               |      left join
               |         ( select sku_id,
               |                  count(*) as score
               |             from mart_waimaiad.recsys_linshou_user_explicit_acts
               |            where dt between ${getDateDelta(dt, -180)} and $dt
               |              and sku_id is not null
               |              and event_type='order'
               |              group by 1) mv
               |      on mv.sku_id=info.sku_id
               |      where info.dt=$dt
               |        and info.sku_id is not null
               |""".stripMargin).rdd.map { row =>
            val sku_id = row.getLong(0)
            val poi_id = row.getLong(1)
            val third_category_id = row.getLong(2)
            val score = row.getLong(3)
            (poi_id, (third_category_id, sku_id, score))
        }.groupByKey.cache()

        val lowN = highQualitySkuTmp.filter( x => x._2.size <= threshold ).mapValues{ _.toList }
        val topN = highQualitySkuTmp.filter( x => x._2.size > threshold )

        val highQualitySku = topN.mapValues{ iter =>
            val skuList = ArrayBuffer[(Long, Long, Long)]()
            val result = iter.toArray.groupBy(_._1).mapValues { iterArray => iterArray.sortBy(_._3).reverse }
            val loop = new Breaks
            loop.breakable {
                var i = 0
                while (true) {
                    for (k <- result.values) {
                        if (skuList.size < threshold) {
                            try {
                                skuList.append(k(i))
                            } catch {
                                case _: Exception =>
                            }
                        } else {
                            loop.break()
                        }
                    }
                    i = i + 1
                }
            }
            skuList.toList
        }.union(lowN)
          .flatMap{ case (poi_id, skuInfoList) => skuInfoList
          .map{ case (third_category_id, sku_id, score) => (poi_id, third_category_id, sku_id, score)}
        }.toDF("poi_id", "third_category_id", "sku_id", "score")

        saveAsTable(spark, highQualitySku, "recsys_linshou_pt_poi_skus_high_quality", Map("dt" -> s"$dt"))
    }
}
