package waimai.job.remote.flashbuy.temporal

import waimai.utils.SparkJobs.RemoteSparkJob

import scala.util.control.Breaks._

object log_adt_flashbuy_pv extends RemoteSparkJob {

    override def run(): Unit = {

        val poiInfo = spark.sql(
            s"""
               |SELECT cast(poi_id AS string) AS poi_id,
               |       first_category_id,
               |       city_name
               |   FROM mart_lingshou.aggr_poi_info_dd
               |  WHERE dt=20230420
               |""".stripMargin).rdd.map{ row =>
            val poi_id = row.getAs[String](0)
            val first_category_id = row.getAs[Long](1)
            val city_name = row.getAs[String](2)
            (poi_id, (first_category_id, city_name))
        }.collect.toMap
        val bcPoiInfo = sc.broadcast(poiInfo)
        val result = spark.sql(
            s"""
               |select pvid,
               |       case when categorycode='102620' then '商超频道页'
               |            when categorycode='101578' then '蔬菜水果频道页'
               |            when product='cpcNewHomepage' then '新首页'
               |            else 'other' end as page,
               |            split(substr(poiids, 2, length(poiids)-1), ',') as RecallPois,
               |            split(substr(poifilterlist, 2, length(poifilterlist)-1), ',') as PoiFilter
               |        FROM log.adt_flashbuy_pv
               |       WHERE dt=20230420
               |         AND (categorycode IN ('102620', '101578') or product='cpcNewHomepage')
               |""".stripMargin
        ).rdd.mapPartitions { iter =>
          val poiInfo = bcPoiInfo.value
          iter.map { row =>
              val pvid = row.getAs[String](0)
              val page = row.getAs[String](1)
              val recallPois = row.getAs[Seq[String]](2)
              val poiFilter = row.getAs[Seq[String]](3)
              val poiList = recallPois.diff(poiFilter)
              var label = 0
              val city_name = poiInfo(poiList.head)._2
              breakable {
                  for (poi <- poiList) {
                      if (poiInfo.getOrElse(poi, (0L, ""))._1 == 10000000L) {
                          label = 1
                          break()
                      }
                  }
              }
              ((page, city_name, label), 1)
          }
        }.reduceByKey(_ + _).collect
        result.foreach{ x =>
            println(x._1._1, x._1._2, x._1._3, x._2)
        }
    }
}
