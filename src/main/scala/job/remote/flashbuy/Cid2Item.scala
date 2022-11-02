package job.remote.flashbuy

import utils.ArrayOperations
import utils.SparkJobs.RemoteSparkJob
import utils.TimeOperations.getDateDelta
import utils.FileOperations.saveAsTable


object Cid2Item extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.beginDt
        val base = spark.sql(
            s"""
               |select concat_ws('_', second_category_id, geohash5) as cate2Id_geohash,
               |       a.poi_id,
               |       sku_id
               |  from mart_waimaiad.recsys_linshou_pt_poi_skus a
               |  join (
               |        select poi_id, geohash5
               |          from mart_lingshou.aggr_poi_info_dd
               |         where dt=$dt
               |) b
               |on a.poi_id=b.poi_id
               |""".stripMargin).rdd.map { row =>
            val cate2Id_geohash = row.getAs[String](0)
            val poi_id = row.getAs[Long](1)
            val sku_id = row.getAs[Long](2)
            (cate2Id_geohash, (poi_id, sku_id, 1L))
        }

        val supplement = spark.sql(
            s"""
               |select concat_ws('_', cid2, poi_geohash) as cate2Id_geohash,
               |       sku_id,
               |       poi_id,
               |       cnt
               |from (
               |      select a.sku_id,
               |             a.poi_id,
               |             poi_geohash,
               |             b.cid2,
               |             count(*) as cnt
               |        from mart_waimaiad.recsys_linshou_user_explicit_acts a
               |        join ( select poi_id, second_category_id as cid2
               |                      from mart_waimaiad.recsys_linshou_pt_poi_skus) b
               |        on a.poi_id=b.poi_id
               |      where dt between ${getDateDelta(dt,-30)} and $dt
               |        and second_category_id is not null
               |        and poi_geohash is not null
               |        and a.sku_id is not null
               |        and event_type in ('click','order','cart')
               |      group by 1,2,3,4)
               |where cnt >= 2
               |""".stripMargin).rdd.map(row => {
            val cate2Id_geohash = row.getAs[String](0)
            val sku_id = row.getAs[Long](1)
            val poi_id = row.getAs[Long](2)
            val cnt = row.getAs[Long](3)
            (cate2Id_geohash, (poi_id, sku_id, cnt))
        })

        val df = base.union(supplement).groupByKey.map { case (k, iter) =>
            val entities = iter.toArray
            val factors = ArrayOperations.logMaxScale(entities.map(_._3.toDouble))
            entities
              .zip(factors)
              .map { case ((poi_id, sku_id, _), cnt) => (poi_id, (sku_id, cnt)) }
              .groupBy(_._1)
              .mapValues(_.map(_._2).maxBy(_._2))
              .values.map{ case(sku_id, score) => s"$sku_id:$score"}
              .toArray
        }.toDF("key", "value")
        val partition = Map("date" -> dt, "branch" -> "cid", "method" -> "pt_cid_sales_sku_base")
        saveAsTable(spark, df, "recsys_linshou_multi_recall_results_v2", partition=partition)
    }
}
