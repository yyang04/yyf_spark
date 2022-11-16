package job.remote.flashbuy

import utils.SparkJobs.RemoteSparkJob
import utils.JSONUtils.{jsonObjectStrToMap, jsonArr2Arr, jsonPv}
import com.alibaba.fastjson.JSONArray

object CoverRate extends RemoteSparkJob{
    override def run(): Unit = {
        // 频道页分召回渠道统计覆盖率比例
        val dt = params.dt
        val hour = params.hour
        spark.sql("""CREATE TEMPORARY FUNCTION get_json_array as 'com.meituan.hive.udf.UDFJsonArray';""")
        // ADD jar viewfs://hadoop-meituan/user/hadoop-hotel/user_upload/hanyecong02_hive-udf.jar;

        val mv = spark.sql(
            s"""
               | select ad_request_id,
               |        poi_id,
               |        split(reserves["spuIdList"], ",") as spuIdList
               |   from mart_waimai_dw_ad.fact_flow_ad_entry_mv mv
               |   join (
               |         select dt,
               |                wm_poi_id
               |  		   from mart_waimai.aggr_poi_info_dd
               | 	      where dt='$dt' and primary_first_tag_id in (10000000,11000000,5018,12000000,13000000,40000000,41000000,15000000,42000000,5007,5001,1001,22)) info
               |   on mv.dt=info.dt and mv.poi_id=info.wm_poi_id
               |        where mv.dt='$dt' and is_valid='PASS'
               |          and split(reserves["spuIdList"], ",") is not null
               |   	      and slot in (191, 201)
               |          and cast(hour as int) >= $hour
               |""".stripMargin).rdd.flatMap{ row =>
            val ad_request_id = row.getAs[String](0)
            val poi_id = row.getAs[Long](1)
            val spuIdList = row.getAs[Seq[String]](2).toArray.map(_.toLong)
            spuIdList.map(spuId => (spuId, (ad_request_id, poi_id)))
        }

        val spu_sku_map = spark.sql(
            s"""
               |select distinct sku_id, product_spu_id
               |from mart_waimaiad.recsys_linshou_pt_poi_skus where dt='$dt'
               |""".stripMargin
        ).rdd.map { row =>
            val sku_id = row.getAs[Long](0)
            val spu_id = row.getAs[Long](1)
            (spu_id, sku_id)
        }

        val mv_tmp = mv
          .join(spu_sku_map)
          .map { case (spuId, (request_poi, sku_id)) => (request_poi, sku_id) }
          .groupByKey.mapValues(_.toArray).map{
            case ((request, poi), sku_id_list) => (poi, (request, sku_id_list))
        }

        val custom = spark.sql(
            s"""
               |select *
               |  from (select wm_poi_id,
               |               get_json_object(b.item, "$$.skuId") as skuId
               |          from origindb_ss.waimaibizadmateriel_bizad_materiel__wm_ad_muses_creative_library a
               |          lateral view explode(get_json_array(content, "$$.product")) b as item
               |          where dt=$dt)
               |where skuId is not null
               |""".stripMargin).rdd.map{ row =>
            val poi = row.getAs[Long](0)
            val skuId = row.getAs[String](1).toLong
            (poi, Array(skuId))
        }.reduceByKey(_++_)

        val mv_custom = mv_tmp.leftOuterJoin(custom).map {
            case (poi, ((request, sku_id_list), v2)) =>
                val customSku = v2.getOrElse(Array[Long]())
                (request, (poi, sku_id_list, customSku))
        }

        val pv = spark.sql(
            s"""
               |select pvid,
               |       case when (array_contains(pv.exp_id, '184594') and array_contains(pv.exp_id, '185967')) then 'base'
               |           	when (array_contains(pv.exp_id, '184594') and array_contains(pv.exp_id, '185966')) then 'uuid'
               |           	when (array_contains(pv.exp_id, '184462') and array_contains(pv.exp_id, '185967')) then 'cid'
               |           	when (array_contains(pv.exp_id, '184462') and array_contains(pv.exp_id, '185966')) then 'cid_uuid'
               |           	when (array_contains(pv.exp_id, '144570') and array_contains(pv.exp_id, '185967')) then 'cid_sku'
               |           	when (array_contains(pv.exp_id, '144570') and array_contains(pv.exp_id, '185966')) then 'cid_sku_uuid'
               |            else 'other' end as exp_id,
               |        recallresults
               |   from (
               |      select pvid,
               |             get_json_object(expids, '$$.frame_exp_list') exp_id,
               |             recallresults
               |        from log.adt_multirecall_pv
               |       where dt='$dt' and scenetype='2'
               |         and cast(hour as int) >= $hour) pv
               | )
               |""".stripMargin).rdd.map{ row =>
            val pvId = row.getAs[String](0)
            val exp_id = row.getAs[String](1)
            val recallResults = row.getAs[String](2)
            val parseResults = jsonPv(recallResults).values.flatten.toMap
              .mapValues ( _.split(",").toSet )
            ((pvId, exp_id), parseResults)
        }.reduceByKey(_++_).map{ case ((pvId, exp_id), parseResults) => (pvId, (exp_id, parseResults)) }

        val res = mv_custom.join(pv).map{
            case (request, (v1, v2)) =>
                val (poi, sku_id_list, customSku) = v1
                val (exp_id, parseResults) = v2
                val tc = customSku.map((_, Set("custom"))).toMap
                val fullPool = parseResults.foldLeft(tc)(
                    (mergedMap, kv) => mergedMap + (kv._1 -> (mergedMap.getOrElse(kv._1, Set()) ++ kv._2))
                ).mapValues(_.toArray.sorted.mkString(","))
                val result_sku = sku_id_list.map (fullPool.getOrElse(_, "nomatch")).groupBy(identity).mapValues(_.length) + ("total" -> sku_id_list.length)
                val result_pv = sku_id_list.map (fullPool.getOrElse(_, "nomatch")).groupBy(identity).mapValues(_ => 1) + ("total" -> 1)
                (exp_id, (result_sku, result_pv))
        }.reduceByKey{
            case ((rs_x, rp_x), (rs_y, rp_y)) =>
                (mergedMap(rs_x, rs_y), mergedMap(rp_x, rp_y))
        }.collect()

        // printResult
        res.foreach{
            case (exp_id, (result_sku, result_pv)) =>
                println(s"""exp_id: $exp_id""")
                println(s"""sku: ${result_sku.mkString(",")}""")
                println(s"""pv: ${result_pv.mkString(",")}""")
        }
    }

    def mergedMap(x: Map[String, Int], y: Map[String, Int]) : Map[String, Int] = {
        x.foldLeft(y)(
            (mergedMap, kv) => mergedMap + (kv._1 -> (mergedMap.getOrElse(kv._1, 0) + kv._2))
        )
    }
}
