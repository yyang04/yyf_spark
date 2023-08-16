package waimai.job.remote.flashbuy.recall.s2i

import waimai.utils.FileOp
import waimai.utils.SparkJobs.RemoteSparkJob

object AoiSearchLog extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.dt
        val bcReader = sc.broadcast(AoiUtil.getAoiInstance)
        val df = spark.sql(
            s"""
               |select coalesce(attribute['label_word'], attribute['key_word']) as word,
               |       source_info,
               |       longitude,
               |       latitude
               |  from mart_waimai.fact_flow_sdk_entry_mv
               | where dt='$dt'
               |   and event_id='b_G73OZ'
               |""".stripMargin).rdd.mapPartitions{ iter =>
                val reader = bcReader.value
                iter.map { row =>
                    val word = row.getAs[String](0)
                    val longitude = row.getAs[Double](2)
                    val latitude = row.getAs[Double](3)
                    val aoiInfo = reader.searchAoi(longitude, latitude, classOf[Aoi])
                    (aoiInfo.aoi_type_name, word)
            }
        }.toDF("aoi_type_name", "word")
        FileOp.saveAsTable(df, "pt_aoi_word", Map("dt" -> dt))
    }
}
