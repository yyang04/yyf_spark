package waimai.job.remote.flashbuy.recall.c2i

import waimai.utils.DateOp.{getNDaysAgo, getNDaysAgoFrom}
import waimai.utils.FileOp.saveAsTable
import waimai.utils.SparkJobs.RemoteSparkJob


object CidBase2Item extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.dt
        val threshold = params.threshold match { case 0 => 2; case x => x }

        print(dt, threshold)

        val base = spark.sql(
            s"""
               |select poi_id,
               |       second_category_id,
               |       third_category_id,
               |       sku_id,
               |       price
               |  from mart_waimaiad.recsys_linshou_pt_poi_skus
               |where dt=$dt
               |""".stripMargin).rdd.flatMap { row =>
            val poiId = row.getAs[Long](0)
            val cate2 = row.getAs[Long](1)
            val cate3 = row.getAs[Long](2)
            val sku_id = row.getAs[Long](3)
            val price = row.getAs[Double](4)
            val v1 = (s"${poiId}_${cate2}", (sku_id, price))
            val v2 = (s"${poiId}_${cate3}", (sku_id, price))
            Array(v1, v2)
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
               |                             when 'cart' then 1
               |                             when 'order' then 10
               |                             else 0 end as cnt
               |        from mart_waimaiad.recsys_linshou_user_explicit_acts a
               |        join (
               |               select sku_id,
               |                      second_category_id as cid2
               |                      from mart_waimaiad.recsys_linshou_pt_poi_skus
               |                where dt=$dt
               |        ) b on a.sku_id=b.sku_id
               |      where dt between ${ getNDaysAgoFrom(dt,60) } and $dt
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
        }.groupByKey.mapValues{_.toArray.sortBy(-_._2).take(threshold)}

        val df = base.fullOuterJoin(supplement).mapValues{ case (v1, v2) =>
            val left = v1.getOrElse(Array())
            val right = v2.getOrElse(Array())
            val tmp = (left ++ right).sortBy(-_._2).take(threshold)
            val value = tmp.length match {
                case 1 => Map(tmp.apply(0)._1 -> 1.0f)
                case 2 => Map(tmp.apply(0)._1 -> 1.0f, tmp.apply(1)._1 -> 0.1f)
            }
            value
        }.toDF("key", "value")

        val partition = Map("dt" -> dt, "table_name" -> "pt_cid2sku", "method_name" -> "pt_cid_sales_sku_base")
        saveAsTable(df, "pt_multi_recall_results_xxx2sku", partition=partition)
    }
}
