package waimai.job.remote.flashbuy.recall.s2i

import waimai.utils.DateUtils.{getNDaysAgo, getNDaysAgoFrom}
import waimai.utils.SparkJobs.RemoteSparkJob

object Discount2Item extends RemoteSparkJob {

    override def run(): Unit = {
        val dt = params.dt match { case "" => getNDaysAgo(1); case x => x }
        spark.sql(
            s"""
               |select poi_id,
               |       sku_id,
               |       second_category_id,
               |       cnt
               |  from (
               |        select poi_id,
               |               sku_id,
               |               second_category_id,
               |               category_name,
               |               category_sec_name,
               |               cnt,
               |               rank() over (partition by poi_id order by cnt desc) as rank
               |          from (
               |                select poi_id,
               |                       sku_id,
               |                       second_category_id,
               |                       max(attribute['category_name']) as category_name,
               |                       max(attribute['category_sec_name']) as category_sec_name,
               |                       count(1) as cnt
               |                  from mart_lingshou.fact_flow_sdk_product_mv mv
               |                  join (
               |                        select sku_id as wm_sku_id,
               |                               second_category_id
               |                          from mart_waimaiad.recsys_linshou_pt_poi_skus
               |                         where dt=$dt
               |                  ) info on mv.poi_id=info.wm_sku_id
               |                 where dt between ${getNDaysAgoFrom(dt, 7)} and $dt
               |                   and event_id='b_xU9Ua'
               |                   and attribute['category_name'] in ('活动')
               |                   and attribute['category_sec_name'] in ('爆品', '折扣')
               |                 group by 1,2,3
               |               )
               |       )
               | where rank <= 20
               |""".stripMargin).rdd.map{ row =>
            val poi_id = row.getAs[Long](0)
            val sku_id = row.getAs[Long](1)
            val second_category_id = row.getAs[Long](2)
            val cnt = row.getAs[Long](3)
            (poi_id, sku_id, second_category_id, cnt)
        }

    }




}
