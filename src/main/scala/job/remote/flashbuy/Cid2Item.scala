package job.remote.flashbuy

import utils.ArrayOperations
import utils.SparkJobs.RemoteSparkJob
import utils.TimeOperations.getDateDelta
import utils.FileOperations.saveAsTable
import scala.collection.mutable


object Cid2Item extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.beginDt
        val threshold = params.threshold
        val threshold2 = params.threshold2

        spark.sql("CREATE TEMPORARY FUNCTION mt_geohash AS 'com.sankuai.meituan.hive.udf.UDFGeoHash'")
        val base = spark.sql(
            s"""
               |select concat_ws('_', second_category_id, mt_geohash(b.latitude /1000000.0, b.longitude /1000000.0, 5)) as cate2Id_geohash,
               |       a.poi_id,
               |       sku_id
               |  from mart_waimaiad.recsys_linshou_pt_poi_skus a
               |  join (
               |        select dt,
               |               poi_id,
               |               latitude,
               |               longitude
               |          from mart_lingshou.aggr_poi_info_dd
               |         where dt=$dt ) b
               |on a.poi_id=b.poi_id and a.dt=b.dt
               |where a.dt=$dt
               |""".stripMargin).rdd.map { row =>
            val cate2Id_geohash = row.getAs[String](0)
            val poi_id = row.getAs[Long](1)
            val sku_id = row.getAs[Long](2)
            (cate2Id_geohash, (poi_id, sku_id))
        }.groupByKey.mapValues { iter =>
            iter.groupBy(_._1).mapValues(x => x.take(threshold2).toArray.map(x => (x._2, 1L)))
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
               |        join ( select sku_id,
               |                      second_category_id as cid2
               |                      from mart_waimaiad.recsys_linshou_pt_poi_skus) b
               |        on a.sku_id=b.sku_id
               |      where dt between ${ getDateDelta(dt,-30) } and $dt
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
        }).groupByKey.mapValues { iter =>
            iter.groupBy(_._1).mapValues { x =>
                x.toArray.sortBy(_._3).takeRight(threshold2).map(x => (x._2, x._3))
            }
        }

        val df = base.fullOuterJoin(supplement).map{ case (k, (v1, v2)) =>
            // v1 => poi_1 -> Array(sku_1, 1), Array(sku_2, 1)
            val left = v1.getOrElse(Map())
            val right = v2.getOrElse(Map())
            val tmp = left.foldLeft(right){
                case (mergedMap, (k, v)) =>
                    mergedMap ++ Map(k -> (mergedMap.getOrElse(k, Array()) ++ v))
            }.mapValues(x => x.sortBy(_._2).takeRight(threshold2)).values.toArray.flatten

            val factors = ArrayOperations.logMaxScale(tmp.map(_._2.toDouble))
            val value = tmp.map(_._1).zip(factors).sortBy(_._2).takeRight(threshold)
              .map{ case(sku_id, score) => s"$sku_id:$score" }
            (k, value)
        }.toDF("key", "value")

        val partition = Map("date" -> dt, "branch" -> "cid", "method" -> "pt_cid_sales_sku_base")
        saveAsTable(spark, df, "recsys_linshou_multi_recall_results_v2", partition=partition)
    }
}
