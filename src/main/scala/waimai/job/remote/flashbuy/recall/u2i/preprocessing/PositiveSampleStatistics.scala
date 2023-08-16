package waimai.job.remote.flashbuy.recall.u2i.preprocessing

import waimai.utils.DateUtils.getNDaysAgoFrom
import waimai.utils.SparkJobs.RemoteSparkJob

object PositiveSampleStatistics extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.dt
        val window = params.window


        val pos = spark.sql(
            s"""
               |select poi_id,
               |       sku_id,
               |       count(*) as cnt
               |  from mart_waimaiad.pt_multirecall_sample mv
               | where dt between '${getNDaysAgoFrom(dt, window)}' and '$dt'
               |  group by 1,2
               |""".stripMargin).rdd.map{ row =>
            val poi_id = row.getAs[Long](0)
            val sku_id = row.getAs[Long](1)
            val cnt = row.getAs[Long](2)
            (poi_id, (sku_id, cnt + 1))
        }.groupByKey.mapValues(_.toMap)

        val sup = spark.sql(
            s"""
               |select poi_id, product_id
               |  from mart_lingshou.dim_prod_product_sku_s_snapshot
               |  WHERE dt='$dt'
               |    AND sell_status=0
               |    AND product_status=0
               |    AND is_valid=1
               |    AND is_online_poi_flag=1
               |""".stripMargin).rdd.map { row =>
            val poi_id = row.getAs[Long](0)
            val sku_id = row.getAs[Long](1)
            (poi_id, (sku_id, 1L))
        }.groupByKey.mapValues(_.toMap)

//        val weightedSku = pos.join(sup).mapValues{
//            case (v1, v2) => v1.foldLeft(v2) ((mergedMap, kv) => mergedMap ++ (kv._1 -> kv._2))
//        }


    }
}
