package waimai.job.remote.flashbuy.predict

import waimai.utils.JsonOp
import waimai.utils.SparkJobs.RemoteSparkJob
import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable.ArrayBuffer

object CtrSample extends RemoteSparkJob {

	def registerUDFs(): Unit = {
		// ADD jar viewfs://hadoop-meituan/user/hadoop-hotel/user_upload/hanyecong02_hive-udf.jar;
		// Add jar viewfs:///user/hadoop-udf-hub/etl-waimai_algorithm_udf/waimai_algorithm_udf-online.jar;
		spark.sql("""CREATE TEMPORARY FUNCTION get_json_array as 'com.meituan.hive.udf.UDFJsonArray';""")
		spark.sql("""CREATE TEMPORARY FUNCTION pvlog_decode AS 'com.sankuai.meituan.waimai.d.algorithm.platform.feature.job.xt.DecodePvLog';""")
		spark.udf.register("merge_json", mergeJson(_: String))
		spark.udf.register("json_array_parse_kv", jsonArrayParseKv(_: Seq[String], _: String))
		spark.udf.register("ele_idx_in_array", elementIdxInArray(_: Seq[Long], _: Seq[Long]))
	}

	override def run(): Unit = {
		val dt = params.dt
		registerUDFs()

		spark.sql(
			s"""
			   |SELECT coalesce(click_num, 0) as label,
			   |       coalesce(slot_id, 0) as slot_id,
			   |       coalesce(city_id, 0) as city_id,
			   |       coalesce(first_tag_id, 0) as first_tag_id,
			   |       coalesce(second_tag_id, 0) as second_tag_id,
			   |       coalesce(client_type, '') as client_type,
			   |       coalesce(new_ctype, '5') as new_ctype,
			   |       coalesce(user_id, 0) as user_id
			   |       coalesce(uuid, '') as uuid,
			   |       poi_id,
			   |       coalesce(device_type, '') as device_type,
			   |       coalesce(appversion, '') as appversion,
			   |       recommend_spus,
			   |       expose_spus,
			   |       coalesce(feature_values, '{}') as feature_values,
			   |       coalesce(feature_values2, '{}') as feature_values2,
			   |       coalesce(get_json_object(dims, '$$.child_tag_id'), '') as child_tag_id
			   |  from (
			   |        SELECT mv.ad_request_id,
			   |               mv.act,
			   |               mv.uuid,
			   |               mv.user_id,
			   |               mv.poi_id,
			   |               info.first_tag_id,
			   |               info.second_tag_id,
			   |               mv.ctype as client_type,
			   |               case when (ctype='wxapp' or ctype='mt_mp') then '1'
			   |                    when (ctype='dpandroid' or ctype='dpiphone') then '2'
			   |                    when (ctype='mtandroid' or ctype='mtiphone') then '3'
			   |                    when (ctype='android' or ctype='iphone') then '4' else '5' end as new_ctype,
			   |               mv.dtype as device_type,
			   |               mv.appversion,
			   |               mv.city_id,
			   |               mv.operation_time as expose_time,
			   |               cast(mv.slot as int) as slot_id,
			   |               mv.expose_num,
			   |               mv.click_num,
			   |               od.order_num,
			   |               od.revenue,
			   |               merge_json(m.fvs) as feature_values,
			   |               merge_json(m.fvs2) as feature_values2,
			   |               m.dims as dims,
			   |               recommend_spus,
			   |               ele_idx_in_array(split(mv.reserves['spuIdList'], ','), recommend_spus) as expose_spus,
			   |               mv.reserves['spu_id'] as click_spu
			   |          FROM mart_waimai_dw_ad.fact_flow_ad_entry_mv mv
			   |          JOIN (
			   |                select requestid as pv_id,
			   |                       get_json_object(dims, '$$.poi_id') as poi_id,
			   |                       max(dims) as dims,
			   |                       concat_ws('sep', collect_list(pvlog_decode(fvcs))) fvs,
			   |                       concat_ws('sep', collect_list(fvs2)) fvs2
			   |                  from log.wml_modelserver_feature_for_wmadplatinumflashbuy
			   |                 where dt=$dt
			   |                   and business='platinum_ptgmv'
			   |                 group by 1,2
			   |          ) m ON mv.ad_request_id=m.pv_id and mv.poi_id=m.poi_id
			   |          JOIN (
			   |                select pv_id,
			   |                       cast(get_json_object(get_json_array(ad_result_list)[0], '$$.poi_id') as bigint) as poi_id,
			   |                       json_array_parse_kv(get_json_array(get_json_object(get_json_array(ad_result_list)[0], '$$.mutable_recommend_sku_infos')), "spu_id") as recommend_spus
			   |                  from log.adt_flashbuy_platinum_pv_v1
			   |                 where dt=$dt
			   |                   and execute_status=0
			   |                   group by 1,2,3
			   |          ) pv ON mv.ad_request_id=pv.pv_id
			   |          JOIN (
			   |                select poi_id,
			   |                       first_category_id as first_tag_id,
			   |                       second_category_id as second_tag_id
			   |                  from mart_lingshou.aggr_poi_info_dd
			   |                 where dt=$dt
			   |          ) info ON mv.poi_id=info.poi_id
			   |          LEFT JOIN (
			   |                SELECT ad_request_id,
			   |                       sub_ord_num as order_num,
			   |                       sub_mt_charge_fee+sub_total as revenue
			   |                  FROM mart_waimai_dw_ad.fact_flow_ad_sdk_entry_order_view
			   |                 WHERE dt=$dt
			   |                   AND is_ad=1
			   |                   AND is_push=1
			   |          ) od ON mv.ad_request_id=od.ad_request_id and mv.act=2
			   |          WHERE mv.dt=$dt
			   |            AND mv.is_valid='PASS'
			   |            AND mv.slot in (160,162,192,193,195)
			   |         )
			   | where row_number() over (partition by ad_request_id, uuid, poi_id order by act) = 1
			   |""".stripMargin)
	}

	// 合并结果
	def mergeJson(data: String): String = {
		if (data.contains("sep")) {
			val arr = data.split("sep").map(x => JsonOp.jsonObjectStrToMap[String](x)).reduce((x, y) => x ++ y)
			JsonOp.iterableToJsonObjectStr(arr)
		} else {
			data
		}
	}

	// 解析Json数组
	def jsonArrayParseKv(arr: Seq[String], key: String): Seq[Long] = {
		if (arr == null) {
			Seq[Long]()
		} else {
			arr.map { x =>
				val jb = JSON.parseObject(x)
				if (jb.containsKey(key)) jb.get(key).toString.toLong else 0L
			}
		}
	}

	// 获取元素在数组的索引
	def elementIdxInArray(arr1: Seq[Long], arr2: Seq[Long]): Seq[Int] = arr1.map { arr2.indexOf(_) }



}
