package waimai.job.remote.flashbuy.feature

import waimai.utils.DateOp.getNDaysAgo
import waimai.utils.SparkJobs.RemoteSparkJob

object PtSkuClick extends RemoteSparkJob {


    override def run(): Unit = {
	    val beginDt = params.beginDt
	    val endDt = params.endDt

        spark.sql(
            s"""
			   |select
			   |  from mart_waimai.fact_flow_sdk_entry_mv mv
			   |  join (
			   |     select poi_id, first_category_id, second_category_id, brand_id,
			   |       from mart_lingshou.aggr_poi_info_dd
			   |      where dt between $beginDt and $endDt
			   |  ) info on mv.poi_id=info.poi_id and mv.dt=info.dt
			   |  where mv.dt between $beginDt and $endDt
			   |    and is_poi=1
			   |    and is_product=0
			   |    and uuid is not null and uuid != ""
               |""".stripMargin)
    }
}
