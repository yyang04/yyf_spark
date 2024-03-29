package waimai.job.remote.flashbuy.recall.v2i.sample

import waimai.utils.DateOp.getNDaysAgoFrom
import waimai.utils.FileOp
import waimai.utils.SparkJobs.RemoteSparkJob

import scala.util.Random

object sample_v1 extends RemoteSparkJob {

    override def run(): Unit = {
        val dt = params.dt
        val threshold = params.threshold

        val sku_pool = spark.sql(
            s"""
               |SELECT distinct poi_id
               |  FROM mart_waimaiad.pt_flashbuy_expose_poi_daily_v1
               | WHERE dt BETWEEN '${getNDaysAgoFrom(dt, 10)}' AND '$dt'
               |""".stripMargin).rdd.map { row =>
            val poi_id = row.getLong(0)
            (poi_id, 0L)
        }

        val sku_pos = spark.sql(
            s"""
               |SELECT event_type,
               |       request_id,
               |       uuid,
               |       user_id,
               |       sku_id,
               |       cast(spu_id as bigint) as spu_id,
               |       poi_id
               |  FROM mart_lingshou.fact_flow_sdk_product_mv
               | WHERE dt='$dt'
               |   AND uuid is not null
               |   AND uuid != ''
               |   AND NOT (sku_id is null AND spu_id is null)
               |   AND poi_id is not null
               |   AND category_type = 13
               |   AND event_id = 'b_xU9Ua'
               |""".stripMargin
        ).rdd.map{ row =>
            val event_type = row.getString(0)
            val request_id = row.getString(1)
            val uuid = row.getString(2)
            val user_id = row.getAs[String](3)
            val sku_id = row.getAs[Long](4)
            val spu_id = row.getAs[Long](5)
            val poi_id = row.getAs[String](6).toLong
            (poi_id, (event_type, request_id, uuid, user_id, sku_id, spu_id))
        }.distinct.join(sku_pool).mapValues{ case(x, _) => x }.cache

        val sku_neg = spark.sql(
            s"""
               |SELECT product_id, product_spu_id, poi_id
               |  FROM mart_lingshou.dim_prod_product_sku_s_snapshot mv
               | WHERE dt = '$dt'
               |   AND sell_status = '0'
               |   AND product_status = '0'
               |   AND is_valid = 1
               |   AND is_online_poi_flag = 1
               |""".stripMargin).rdd.map { row =>
            val sku_id = row.getAs[Long](0)
            val spu_id = row.getAs[Long](1)
            val poi_id = row.getLong(2)
            (poi_id, (sku_id, spu_id))
        }.join(sku_pool).mapValues{ case(x, _) => x }.groupByKey

        val df = sku_pos.join(sku_neg).flatMap{
            case (poi_id, (v1, v2)) =>
                val (event_type, request_id, uuid, user_id, sku_id, spu_id) = v1
                Random.shuffle(v2).take(threshold).map {
                    case (sku_id, spu_id) => (poi_id, ("view", request_id, uuid, user_id, sku_id, spu_id))
                }
        }.union(sku_pos).map{
            case (poi_id, (event_type, request_id, uuid, user_id, sku_id, spu_id)) =>
            (poi_id, event_type, request_id, uuid, user_id, sku_id, spu_id)
        }.toDF("poi_id", "event_type", "request_id", "uuid", "user_id", "sku_id", "spu_id")

        FileOp.saveAsTable(df, "pt_sg_u2i_sample_v1", Map("dt" -> s"$dt"))
    }

}
