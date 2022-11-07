package job.remote.flashbuy

import utils.SparkJobs.RemoteSparkJob
import play.api.libs.json._

import scala.collection.mutable
import scala.reflect.ClassTag
import com.alibaba.fastjson.{JSON, JSONArray}

object CoverRate extends RemoteSparkJob{
    override def run(): Unit = {
        val dt = params.dt

        val mv = spark.sql(
            s"""
               | select ad_request_id, split(reserves["spuIdList"], ",") as spuIdList
               |   from mart_waimai_dw_ad.fact_flow_ad_entry_mv mv
               |      join (select dt as info_dt,
               |                   wm_poi_id
               |  		      from mart_waimai.aggr_poi_info_dd
               | 	         where dt = '$dt'
               |                   and primary_first_tag_id in
               |         (10000000,11000000,5018,12000000,13000000,40000000,41000000,15000000,42000000,5007,5001,1001,22)) info
               |      on mv.dt=info_dt and mv.poi_id=info.wm_poi_id
               |   where mv.dt = '$dt' and is_valid = 'PASS'
               |     and split(reserves["spuIdList"], ",") is not null
               |   	 and slot in (191, 201)
               |""".stripMargin).rdd.flatMap{ row =>
            val ad_request_id = row.getAs[String](0)
            val spuIdList = row.getAs[Seq[String]](1).toArray.map(_.toLong)
            spuIdList.map(spuId => (ad_request_id, spuId)).map(_.swap)
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

        val mv_tmp = mv.join(spu_sku_map).map{ case (k, (v1, v2)) => (v1, v2) }.groupByKey.mapValues{ _.toArray}

        val pv = spark.sql(
            s"""select pvid,
               |       recallresults
               |  from log.adt_multirecall_pv
               | where dt='$dt' and scenetype='2'
               |""".stripMargin).rdd.map{ row =>
            val pvid = row.getAs[String](0)
            val a = row.getAs[String](1)
            val b = jsonObjectStrToMap[JSONArray](a)
              .map(x => (x._1, jsonArr2Arr[JSONArray](x._2).map{ y =>
                  try {
                      (y.getString(0).toLong, y.getString(1))
                  } catch  {
                      case _: Exception => (0, "")
                  }
              }))
            val c = b.values.flatten.toMap
            (pvid, c)
        }


        val res = mv_tmp.leftOuterJoin(pv).map{ case (k, (v1, v2)) =>
            val total = v1.length
            val hit = v2 match {
                case Some(d) => v1.map(x => d.getOrElse(x, "empty")).count(x => x != "salesku" && x != "empty")
                case _ => 0
            }
            (total, hit)
        }.reduce((x,y) => (x._1 + y._1, x._2 +y._2))

        println(s"total:${res._1}, hit:${res._2}, ratio: ${res._2/res._1}")
    }

    def jsonObjectStrToMap[T: ClassTag](json: String): Map[String, T] = {
        val arr = new mutable.HashMap[String, T]()
        try {
            val jb = JSON.parseObject(json)
            val kSet = jb.entrySet().iterator()
            while (kSet.hasNext) {
                val kv = kSet.next()
                val k = kv.getKey
                val v = kv.getValue.asInstanceOf[T]
                arr.put(k, v)
            }
        } catch {
            case e: Exception => println("parse jsonobject error")
        }
        arr.toMap
    }

    def jsonArr2Arr[T: ClassTag](ja: JSONArray): Array[T] = {
        val n = ja.size()
        val rs = new Array[T](n)
        for (i <- 0 until n) {
            rs(i) = ja.get(i).asInstanceOf[T]
        }
        rs
    }
}
