package job.remote.flashbuy

import utils.ArrayOperations
import utils.SparkJobs.RemoteSparkJob
import utils.TimeOperations.getDateDelta
import utils.FileOperations.saveAsTable


object Cid2Item extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.beginDt
        val df = spark.sql(
            s"""
               |select concat_ws('_', cid3, poi_geohash) as cate3Id_geohash,
               |       sku_id,
               |       cnt
               |from (
               |      select a.sku_id, poi_geohash, b.cid3, count(*) as cnt
               |        from mart_waimaiad.recsys_linshou_user_explicit_acts a
               |        join ( select sku_id, third_category_id as cid3
               |                      from mart_waimaiad.recsys_linshou_pt_poi_skus) b
               |        on a.sku_id=b.sku_id
               |      where dt between ${getDateDelta(dt,-30)} and $dt
               |        and third_category_id is not null
               |        and poi_geohash is not null
               |        and a.sku_id is not null
               |        and event_type in ('click','order','cart')
               |      group by 1,2,3)
               |where cnt >= 3
               |""".stripMargin).rdd.map(row => {
            val cate3Id_geohash = row.getAs[String](0)
            val sku_id = row.getAs[Long](1)
            val cnt = row.getAs[Long](2)
            (cate3Id_geohash, (sku_id, cnt))
        }).groupByKey.mapValues(iter => {
            val entities = iter.toArray.sortBy(_._2).takeRight(100)
            val factors = ArrayOperations.maxScale(entities.map(_._2.toDouble))
            val results = entities.map(_._1).zip(factors).map(x=>s"${x._1}:${x._2}")
            results
        }).toDF("key", "value")
        val partition = Map("date" -> dt, "branch" -> "cid", "method" -> "pt_cid_sales_sku_base")
        saveAsTable(spark, df, "recsys_linshou_multi_recall_results_v2", partition=partition)
    }
}
