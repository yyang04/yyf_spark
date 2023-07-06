package waimai.job.remote.flashbuy.yx

import waimai.utils.FileOp
import waimai.utils.SparkJobs.RemoteSparkJob
import waimai.utils.DateUtils.getNDaysAgoFrom

object group extends RemoteSparkJob {

    override def run(): Unit = {
        val endDt = params.endDt
        val window = params.window
        val beginDt = getNDaysAgoFrom(endDt, window)

        val df = spark.sql(
            s"""
               |select uuid
               |  from mart_waimai.fact_flow_sdk_entry_mv mv
               |  join (
               |    select dt, poi_id
               |      from mart_lingshou.aggr_poi_info_dd
               |     where dt between $beginDt and $endDt
               |  ) info on mv.poi_id=info.poi_id and mv.dt=info.dt
               |  where mv.dt between $beginDt and $endDt
               |    and event_type='click'
               |    and is_poi=1
               |    and mv.poi_id is not null and uuid is not null
               |    group by 1
               |""".stripMargin).rdd.map{ row =>
            val uuid = row.getAs[String](0)
            uuid
        }.toDF("uuid")
        FileOp.saveAsTable(spark, df, "pt_sg_uuid_not_click", partition=Map("dt" -> endDt, "window" -> window))
    }



}
