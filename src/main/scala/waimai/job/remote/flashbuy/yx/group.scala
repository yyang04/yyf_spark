package waimai.job.remote.flashbuy.yx

import waimai.utils.FileOp
import waimai.utils.SparkJobs.RemoteSparkJob

class group extends RemoteSparkJob {

    override def run(): Unit = {
        val df = spark.sql(
            s"""
               |select uuid,
               |  from mart_waimai.fact_flow_sdk_log_view mv
               |  join (
               |    select dt, poi_id,
               |      from mart_lingshou.aggr_poi_info_dd
               |     where dt between 20230601 and 20230701
               |  ) info on mv.poi_id=info.poi_id and mv.dt=info.dt
               |  where mv.dt between 20230601 and 20230701
               |    and partition_view=0
               |    and log_type=1
               |    and mv.poi_id is not null and uuid is not null
               |""".stripMargin).rdd.map{ row =>
            val uuid = row.getAs[String](0)
            uuid
        }.toDF("uuid")
        FileOp.saveAsTable(spark, df, "pt_sg_uuid_not_click", partition=Map("dt" -> "20230701", "window" -> "30"))
    }
}
