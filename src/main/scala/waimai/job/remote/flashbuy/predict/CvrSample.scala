package waimai.job.remote.flashbuy.predict


import waimai.utils.{FileOp, JsonOp}
import waimai.utils.SparkJobs.RemoteSparkJob

object CvrSample extends RemoteSparkJob {
	override def run(): Unit = {
		// ADD jar viewfs://hadoop-meituan/user/hadoop-hotel/user_upload/hanyecong02_hive-udf.jar;
		// Add jar viewfs:///user/hadoop-udf-hub/etl-waimai_algorithm_udf/waimai_algorithm_udf-online.jar;
		spark.sql("""CREATE TEMPORARY FUNCTION get_json_array as 'com.meituan.hive.udf.UDFJsonArray';""")
		spark.sql("""CREATE TEMPORARY FUNCTION pvlog_decode AS 'com.sankuai.meituan.waimai.d.algorithm.platform.feature.job.xt.DecodePvLog';""")
		spark.udf.register("merge_json", mergeJson(_: String))

		val dt = params.dt
		val tableName = params.tableName
		val df = spark.sql(
			s"""
			   |SELECT mv.ad_request_id,
			   |       mv.act,
			   |       mv.uuid,
			   |       mv.user_id,
			   |       mv.poi_id,
			   |       info.first_tag_id,
			   |       info.second_tag_id,
			   |       mv.operation_time as expose_time,
			   |       cast(mv.slot as string) as slot_id,
			   |       mv.click_num as click,
			   |       m.feature_values,
			   |       m.feature_values2,
			   |       m.dims as dims,
			   |       coalesce(sub_ord_num, 0) as order_num,
			   |       coalesce(sub_mt_charge_fee + sub_total) as revenue
			   |  FROM mart_waimai_dw_ad.fact_flow_ad_entry_mv mv
			   |  JOIN (
			   |        select requestid as pv_id,
			   |               get_json_object(dims, '$$.poi_id') as poi_id,
			   |               max(dims) as dims,
			   |               merge_json(concat_ws('sep', collect_list(pvlog_decode(fvcs)))) as feature_values,
			   |               merge_json(concat_ws('sep', collect_list(fvs2))) as feature_values2
			   |          from log.wml_modelserver_feature_for_wmadplatinumflashbuy
			   |         where dt=$dt
			   |           and business='platinum_ptgmv'
			   |         group by 1,2
			   |  ) m ON mv.ad_request_id=m.pv_id and mv.poi_id=m.poi_id
			   |  JOIN (
			   |        select poi_id,
			   |               first_category_id as first_tag_id,
			   |               second_category_id as second_tag_id
			   |          from mart_lingshou.aggr_poi_info_dd
			   |         where dt=$dt
			   |  ) info ON mv.poi_id=info.poi_id
			   |  LEFT JOIN (
			   |        select dt,
			   |               ad_request_id,
			   |               sub_ord_num,
			   |               sub_mt_charge_fee,
			   |               sub_total
			   |          from mart_waimai_dw_ad.fact_flow_ad_sdk_entry_order_view
			   |         where dt=$dt
			   |           and is_ad=1
			   |           and is_push=1
			   |  ) od on mv.ad_request_id=od.ad_request_id and mv.act=2
			   |  WHERE mv.dt=$dt
			   |    AND mv.is_valid='PASS'
			   |    AND mv.slot in (160,162,192,193,195)
			   |    AND mv.act=2""".stripMargin)

		FileOp.saveAsTable(df, tableName, Map("dt" → dt))
	}

	def mergeJson(data: String): String = {
		if (data.contains("sep")) {
			val arr = data.split("sep").map(x => JsonOp.jsonObjectStrToMap[String](x)).reduce((x, y) => x ++ y)
			JsonOp.iterableToJsonObjectStr(arr)
		} else {
			data
		}
	}
}
