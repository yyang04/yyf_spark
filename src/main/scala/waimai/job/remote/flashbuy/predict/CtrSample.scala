package waimai.job.remote.flashbuy.predict

import waimai.utils.JsonOp
import waimai.utils.SparkJobs.RemoteSparkJob
import com.alibaba.fastjson.{JSON, JSONObject}


object CtrSample extends RemoteSparkJob {

	def registerUDFs(): Unit = {
		// ADD jar viewfs://hadoop-meituan/user/hadoop-hotel/user_upload/hanyecong02_hive-udf.jar;
		// Add jar viewfs:///user/hadoop-udf-hub/etl-waimai_algorithm_udf/waimai_algorithm_udf-online.jar;
		spark.sql("""CREATE TEMPORARY FUNCTION get_json_array as 'com.meituan.hive.udf.UDFJsonArray';""")
		spark.sql("""CREATE TEMPORARY FUNCTION pvlog_decode AS 'com.sankuai.meituan.waimai.d.algorithm.platform.feature.job.xt.DecodePvLog';""")
		spark.udf.register("merge_json", mergeJson(_: String))
		spark.udf.register("json_array_parse_kv", jsonArrayParseKv(_: Seq[String], _: String))
		spark.udf.register("ele_idx_in_array", elementIdxInArray(_: Seq[String], _: Seq[Long]))
	}

	override def run(): Unit = {
		val dt = params.dt
		val tableName = params.tableName
		registerUDFs()

		spark.sql(
			s"""
			   |with cte as (
			   |        SELECT mv.dt,
			   |               mv.ad_request_id,
			   |               mv.act,
			   |               mv.uuid,
			   |               mv.user_id,
			   |               mv.poi_id,
			   |               info.first_tag_id,
			   |               info.second_tag_id,
			   |               mv.operation_time as expose_time,
			   |               cast(mv.slot as string) as slot_id,
			   |               mv.click_num as click,
			   |               m.feature_values,
			   |               m.feature_values2,
			   |               m.dims as dims,
			   |               recommend_spus,
			   |               ele_idx_in_array(split(mv.reserves['spuIdList'], ','), pv.recommend_spus) as expose_spus,
			   |               mv.reserves['spu_id'] as click_spu,
			   |               row_number() over (partition by mv.ad_request_id, mv.uuid, mv.poi_id order by mv.act) as row_num
			   |          FROM mart_waimai_dw_ad.fact_flow_ad_entry_mv mv
			   |          JOIN (
			   |                select requestid as pv_id,
			   |                       get_json_object(dims, '$$.poi_id') as poi_id,
			   |                       max(dims) as dims,
			   |                       merge_json(concat_ws('sep', collect_list(pvlog_decode(fvcs)))) as feature_values,
			   |                       merge_json(concat_ws('sep', collect_list(fvs2))) as feature_values2
			   |                  from log.wml_modelserver_feature_for_wmadplatinumflashbuy
			   |                 where dt=$dt
			   |                   and business='platinum_ptgmv'
			   |                 group by 1,2
			   |          ) m ON mv.ad_request_id=m.pv_id and mv.poi_id=m.poi_id
			   |          JOIN (
			   |                select pv_id,
			   |                       json_array_parse_kv(get_json_array(get_json_object(get_json_array(ad_result_list)[0], '$$.mutable_recommend_sku_infos')), "spu_id") as recommend_spus
			   |                  from log.adt_flashbuy_platinum_pv_v1
			   |                 where dt=$dt
			   |                 group by 1,2
			   |          ) pv ON mv.ad_request_id=pv.pv_id
			   |          JOIN (
			   |                select poi_id,
			   |                       first_category_id as first_tag_id,
			   |                       second_category_id as second_tag_id
			   |                  from mart_lingshou.aggr_poi_info_dd
			   |                 where dt=$dt
			   |          ) info ON mv.poi_id=info.poi_id
			   |          WHERE mv.dt=$dt
			   |            AND mv.is_valid='PASS'
			   |            AND mv.slot in (160,162,192,193,195)
			   |)
			   |
			   |SELECT dt,
			   |       ad_request_id,
			   |       click,
			   |       dims,
			   |       feature_values,
			   |       feature_values2,
			   |       uuid,
			   |       cast(user_id as string) as user_id,
			   |       cast(poi_id as string) as poi_id,
			   |       first_tag_id,
			   |       second_tag_id,
			   |       slot_id,
			   |       expose_time,
			   |       recommend_spus,
			   |       expose_spus
			   |  from cte
			   | where row_num = 1
			   |""".stripMargin)
		  .write
		  .option("path", "viewfs://hadoop-meituan/ghnn07/warehouse/mart_waimaiad.db")
		  .mode("overwrite")
		  .partitionBy("dt")
		  .format("orc")
		  .saveAsTable(tableName)
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
	def elementIdxInArray(arr1: Seq[String], arr2: Seq[Long]): Seq[Int] = {
		arr1 match {
			case null => Seq[Int]()
			case _ => arr2 match {
				case null => Seq.fill[Int](arr1.length)(-1)
				case _ =>
					val arr2Map = arr2.zipWithIndex.toMap
					arr1.map { x =>
						try {
							arr2Map.getOrElse(x.toLong, -1)
						} catch {
							case _: NumberFormatException => -1
						}
					}
			}
		}
	}
}
