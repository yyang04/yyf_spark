package waimai.job.remote.flashbuy.evaluation

import waimai.utils.Json.JSONUtils.jsonPv
import waimai.utils.SparkJobs.RemoteSparkJob


object Evaluation extends RemoteSparkJob{

    override def run(): Unit = {
        val dt = params.dt

        val mv = spark.sql(
            s"""
               | select ad_request_id,
               |        split(reserves["spuIdList"], ",") as spuIdList,
               |        cast(if(act = 3,1,0) as double) as pv_num,
               |        cast(if(act = 2,1,0) as double) as click_num
               |   from mart_waimai_dw_ad.fact_flow_ad_entry_mv mv
               |   join (select dt,
               |                wm_poi_id
               |  		   from mart_waimai.aggr_poi_info_dd
               | 	      where dt = '$dt' and primary_first_tag_id in (10000000,11000000,5018,12000000,13000000,40000000,41000000,15000000,42000000,5007,5001,1001,22)) info
               |      on mv.dt=info.dt and mv.poi_id=info.wm_poi_id
               |   where mv.dt = '$dt' and is_valid = 'PASS'
               |     and split(reserves["spuIdList"], ",") is not null
               |   	 and slot in (191, 201)
               |""".stripMargin).rdd.flatMap { row =>
            val ad_request_id = row.getAs[String](0)
            val spuIdList = row.getAs[Seq[String]](1).toArray.map(_.toLong)
            val pv_num = row.getAs[Double](2)
            val click_num = row.getAs[Double](3)
            spuIdList.map(spuId => (spuId, (ad_request_id, pv_num, click_num)))
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
          .map { case (k, (v1, v2)) => (v1, v2) }
          .groupByKey
          .mapValues(_.toArray)
          .map{ case ((request_id, pv_num, click_num), sku_array) => (request_id, (pv_num, click_num, sku_array)) }

        val pv = spark.sql(
            s"""select pvid,
               |       recallresults
               |  from log.adt_multirecall_pv
               | where dt='$dt' and scenetype='2'
               |""".stripMargin).rdd.map { row =>
            val pvid = row.getAs[String](0)
            val a = row.getAs[String](1)
            val b = jsonPv(a)
            val c = b.values.flatten.toMap
              .mapValues (x => x.split(",").toSet)
            (pvid, c)
        }.reduceByKey(_ ++ _)


        val res = mv_tmp.leftOuterJoin(pv)
          .map { case (request_id, (v1, v2)) =>
              val (pv_num, click_num, skuList) = v1
              val s = v2 match {
                  case Some(d) => skuList.map(x => d.getOrElse(x, Set())).reduce(_++_)
                  case _ => Set[String]()
              }
              s.map(x => x -> (pv_num, click_num)).toMap
          }.reduce {
            case(m1, m2) =>
                val m = m1.foldLeft(m2)(
                    (mergedMap, kv) => {
                        mergedMap + (kv._1 -> (mergedMap.getOrElse(kv._1, (0.0, 0.0))._1 + kv._2._1, mergedMap.getOrElse(kv._1, (0.0, 0.0))._2 + kv._2._2))
                    }
                )
                m
        }
        res.foreach{
            case(method, (pv, click)) =>
                println(s"method:$method: pv:$pv, click:$click, ctr:${click/pv}")
        }

    }
}
