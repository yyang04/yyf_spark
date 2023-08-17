package waimai.job.remote.flashbuy.recall.v2i.preprocessing

import waimai.utils.DateOp.getNDaysAgoFrom
import waimai.utils.SparkJobs.RemoteSparkJob

object PositiveSampleStatistics extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.dt
        val window = params.window

        val posSample = spark.sql(
            s"""
               |select dt, poi_id, sku_id, spu_id, user_id, uuid,
               |       city_id, event_id, event_timestamp, hour, client_id, appversion, is_pt
               |
               |
               |
               |
               |
               |""".stripMargin)

        val skuScore = spark.sql(
            s"""
               |select sku_id,
               |       count(*) as cnt
               |  from mart_waimaiad.pt_multirecall_sample
               | where dt between '${getNDaysAgoFrom(dt, window)}' and '$dt'
               |  group by 1
               |""".stripMargin).rdd.map{ row =>
            val sku_id = row.getAs[Long](0)
            val cnt = row.getAs[Long](1)
            (sku_id, cnt)
        }.collect

        val mixSample = spark.sql(
            s"""
               |
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
