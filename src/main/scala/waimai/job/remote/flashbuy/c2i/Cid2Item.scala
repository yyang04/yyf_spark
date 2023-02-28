package waimai.job.remote.flashbuy.c2i

import waimai.utils.ArrayOperations
import waimai.utils.FileOperations.saveAsTable
import waimai.utils.TimeOperations.getDateDelta
import waimai.utils.SparkJobs.RemoteSparkJob


object Cid2Item extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.beginDt
        val threshold = params.threshold

        val base = spark.sql(
            s"""
               |select distinct concat_ws('_', poi_id, second_category_id) as poi_cate,
               |       sku_id,
               |       price
               |  from mart_waimaiad.recsys_linshou_pt_poi_skus
               |where dt=$dt
               |""".stripMargin).rdd.map { row =>
            val poi_cate = row.getAs[String](0)
            val sku_id = row.getAs[Long](1)
            val price = row.getAs[Double](2)
            (poi_cate, (sku_id, price))
        }.groupByKey.mapValues{_.toArray.sortBy(_._2).take(threshold).map{ case(sku_id, price) => (sku_id, 1L)} }

        val supplement = spark.sql(
            s"""
               |select concat_ws('_', poi_id, cid2) as poi_cate,
               |       sku_id,
               |       sum(cnt) as cnt
               |from (
               |      select a.sku_id,
               |             a.poi_id,
               |             b.cid2,
               |             case event_type when 'click' then 1
               |                             when 'cart' then 2
               |                             else 5 end as cnt
               |        from mart_waimaiad.recsys_linshou_user_explicit_acts a
               |        join ( select sku_id,
               |                      second_category_id as cid2
               |                      from mart_waimaiad.recsys_linshou_pt_poi_skus
               |                where dt=$dt) b
               |        on a.sku_id=b.sku_id
               |      where dt between ${ getDateDelta(dt,-60) } and $dt
               |        and a.sku_id is not null
               |        and event_type in ('click','order','cart')
               |)
               |group by 1,2
               |having cnt > 1
               |""".stripMargin).rdd.map{ row =>
            val poi_cate = row.getAs[String](0)
            val sku_id = row.getAs[Long](1)
            val cnt = row.getAs[Long](2)
            (poi_cate, (sku_id, cnt))
        }.groupByKey.mapValues{_.toArray.sortBy(_._2).takeRight(threshold)}

        val df = base.fullOuterJoin(supplement).mapValues{ case (v1, v2) =>
            val left = v1.getOrElse(Array())
            val right = v2.getOrElse(Array())
            val tmp = (left ++ right).sortBy(_._2).takeRight(threshold).toMap
            val value = tmp.size match {
                case 1 => Array(f"${tmp.toArray.apply(0)._1}:1.0")
                case 2 =>
                    val arr = tmp.toArray.sortBy(_._2).reverse
                    Array(f"${arr.apply(0)._1}:1.0", f"${arr.apply(1)._1}:0.01")
            }
            value
        }.toDF("key", "value")

        val partition = Map("date" -> dt, "branch" -> "cid", "method" -> "pt_cid_sales_sku_base")
        saveAsTable(spark, df, "recsys_linshou_multi_recall_results_v2", partition=partition)
    }
}
