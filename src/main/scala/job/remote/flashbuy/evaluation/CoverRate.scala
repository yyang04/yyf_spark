package job.remote.flashbuy.evaluation

import utils.JSONUtils.jsonPv
import utils.MapOperations.mergeMap
import utils.SparkJobs.RemoteSparkJob

object CoverRate extends RemoteSparkJob{
    override def run(): Unit = {
        // 频道页分召回曝光渠道统计覆盖率比例
        val dt = params.dt
        val hour = params.hour
        val city = params.city

        // ADD jar viewfs://hadoop-meituan/user/hadoop-hotel/user_upload/hanyecong02_hive-udf.jar;
        spark.sql("""CREATE TEMPORARY FUNCTION get_json_array as 'com.meituan.hive.udf.UDFJsonArray';""")

        val city_id = city match {
            case "" => ""
            case x => s"and city_id in ($x)"
        }

        val mv = spark.sql(
            s"""
               | SELECT ad_request_id,
               |        poi_id,
               |        split(reserves["spuIdList"], ",") as spuIdList
               |   FROM mart_waimai_dw_ad.fact_flow_ad_entry_mv mv
               |   JOIN (
               |         select dt,
               |                wm_poi_id
               |  		   from mart_waimai.aggr_poi_info_dd
               | 	      where dt='$dt' and primary_first_tag_id in (10000000,11000000,5018,12000000,13000000,40000000,41000000,15000000,42000000,5007,5001,1001,22)) info
               |    ON mv.dt=info.dt and mv.poi_id=info.wm_poi_id
               |    WHERE mv.dt='$dt'
               |          AND is_valid='PASS'
               |          AND split(reserves["spuIdList"], ",") is not null
               |   	      AND slot in (191, 201)
               |          AND cast(hour as int) >= $hour
               |          $city_id
               |""".stripMargin).rdd.flatMap{ row =>
            val ad_request_id = row.getAs[String](0)
            val poi_id = row.getAs[Long](1)
            val spuIdList = row.getAs[Seq[String]](2).toArray.map(_.toLong)
            spuIdList.map(spuId => (spuId, (ad_request_id, poi_id)))
        }

        val spu_sku_map = spark.sql(
            s"""
               |SELECT product_id,
               |       product_spu_id
               |  FROM mart_lingshou.dim_prod_product_sku_s_snapshot
               | WHERE dt='$dt'
               |   AND sell_status = '0'
               |   AND product_status = '0'
               |   AND is_valid = 1
               |   AND is_online_poi_flag = 1
               |   AND product_spu_id is not null
               |""".stripMargin
        ).rdd.map { row =>
            val sku_id = row.getAs[Long](0)
            val spu_id = row.getAs[Long](1)
            (spu_id, sku_id)
        }.reduceByKey((x ,_)=>x)

        val mv_tmp = mv
          .join(spu_sku_map)
          .map { case (spuId, (request_poi, sku_id)) => (request_poi, Array(sku_id)) }
          .reduceByKey(_++_).map {
            case ((request, poi), sku_id_list) => (poi, (request, sku_id_list))
        }

        val custom = spark.sql(
            s"""
               |select *
               |  from (select wm_poi_id,
               |               get_json_object(b.item, "$$.skuId") as skuId
               |          from origindb_ss.waimaibizadmateriel_bizad_materiel__wm_ad_muses_creative_library a
               |          lateral view explode(get_json_array(content, "$$.productList")) b as item
               |          where dt=$dt)
               |  where skuId is not null
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
            s"""select pvid,
               |       case when pv.exp_id like '%193264%' then 'exp'
               |            when pv.exp_id like '%193265%' then 'base'
               |            else 'other' as exp_id,
               |       recallresults
               |   from (
               |      select pvid,
               |             get_json_object(expids, '$$.frame_exp_list') exp_id,
               |             recallresults
               |        from log.adt_multirecall_pv
               |       where dt='$dt' and scenetype='2'
               |         and cast(hour as int) >= $hour)
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
                val result_pv = Map(sku_id_list.map (fullPool.getOrElse(_, "nomatch")).distinct.sorted.mkString(",") -> 1) + ("total" -> 1)
                (exp_id, (result_sku, result_pv))
        }.reduceByKey{
            case ((rs_x, rp_x), (rs_y, rp_y)) =>
                (mergeMap(rs_x, rs_y), mergeMap(rp_x, rp_y))
        }.collect()

        // printResult
        res.foreach{
            case (exp_id, (result_sku, result_pv)) =>
                println(s"""exp_id: $exp_id""")
                println(s"""sku粒度: ${handleMap(result_sku).mkString(",")}""")
                println(s"""pv粒度: ${handleMap(result_pv).mkString(",")}""")
                println()
        }
    }

    def handleMap(x: Map[String, Int]): Array[(String, String)] = {
        val total = x("total")
        val result = x.toArray.sortBy(_._2).reverse.map{ case (k, v) => (k, f"${v.toDouble/total*100}%.2f%%")}
        Array(("total", f"$total")) ++ result
    }
}
