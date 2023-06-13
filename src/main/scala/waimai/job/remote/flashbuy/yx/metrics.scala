package waimai.job.remote.flashbuy.yx

import waimai.utils.SparkJobs.RemoteSparkJob
import waimai.utils.JsonUtils.jsonObjectStrToArrayMap
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import scala.util.control.Breaks._

object metrics extends RemoteSparkJob {
    val beginDt = params.beginDt
    val endDt = params.endDt
    override def run(): Unit = {
        val mv = spark.sql(
            s"""
               |select ad_request_id,
               |       hour,
               |       poi_id,
               |       act,
               |       is_charge,
               |       final_charge,
               |       sub_ord_num as order_num,
               |       sub_mt_charge_fee,
               |       sub_total
               |  from mart_waimai_dw_ad.fact_flow_ad_entry_mv
               |  join (
               |       select poi_id
               |         from mart_lingshou.aggr_poi_info_dd
               |        where dt between $beginDt and $endDt
               |  ) info on mv.poi_id=info.poi_id and mv.dt=info.dt
               |  left join (
               |        select dt,
               |               ad_request_id,
               |               sub_mt_charge_fee,
               |               sub_total,
               |               sub_ord_num
               |          from mart_waimai_dw_ad.fact_flow_ad_sdk_entry_order_view
               |         where dt between $beginDt and $endDt
               |           and is_push=1
               |  ) od on mv.ad_request_id=od.ad_request_id and mv.dt=od.dt and mv.act=2
               |  where mv.dt between $beginDt and $endDt
               |    and is_valid='PASS'
               |    and slot in (195)
               |""".stripMargin).as[Request].rdd

        val pv = spark.sql(
            s"""
               |select pv_id,
               |       predict_ad_queue
               |  from log.adt_flashbuy_platinum_pv_v1
               | where dt between $beginDt and $endDt
               |""".stripMargin).rdd.map{ row =>
            val ad_request_id = row.getString(0)
            val predict_ad_queue = jsonObjectStrToArrayMap[String](row.getString(1))
            (ad_request_id, predict_ad_queue)
        }.reduceByKey(_++_)

        val tmp = mv.map{ request => (request.ad_request_id, request) }.join(pv).map{
            case(ad_request_id, (request, predict_ad_queue)) =>
                val ad_map = predict_ad_queue.map { ad =>
                    (ad("poi_id"), ad)
                }.toMap
                val gmv = ad_map(request.poi_id.toString)("gmv").toDouble
                (request, gmv)
        }.cache()

        val per = tmp.filter { case (request, gmv) => request.act == 3}.map{ case (request, gmv) => (request.hour, (request, gmv)) }.groupByKey.map{
            case (hour, iter) =>
                val gmvPerHour = iter.map{case (request, gmv) => gmv}
                val percentile = new Percentile
                val gmvPer = Range.inclusive(10, 90, 10).toArray.map{ x => percentile.evaluate(gmvPerHour.toArray, x.toDouble) }
                (hour.toInt, gmvPer)
        }.collect.sortBy(_._1)

        per.foreach{ case (hour, gmvPer) =>
            println(s"Hour:$hour, ${gmvPer.mkString(",")}")
        }

        tmp.map {
            case (request, gmv) => (grade(request.hour.toInt, gmv, per), request)
        }.groupByKey.map{
            case (grade, iter) =>
                val result = iter.map{ case Request(ad_request_id, hour, poi_id, act, is_charge, final_charge, sub_order_num, sub_total, sub_mt_charge_fee) =>
                    val view_num = if (act == 3) 1 else 0
                    val click_num = if(act == 2) 1 else 0
                    val charge = if(is_charge == 1) final_charge else 0
                    val order_num = sub_order_num
                    val price = sub_total + sub_mt_charge_fee
                    (view_num, click_num, charge, order_num, price)
                }.reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5))
                (grade, result)
        }.collect.foreach{ x =>
            println(s"${x._1}, ${x._2._1}, ${x._2._2}, ${x._2._3}, ${x._2._4}, ${x._2._5}")
        }
    }

    def grade(hour: Int, gmv: Double, gmvPerHour: Array[(Int, Array[Double])]): Int = {
        val gmvPerHourMap = gmvPerHour.toMap
        val gmvPer = gmvPerHourMap(hour)
        var resultIndex = gmvPerHour.length
        breakable {
            for ((gmvGrid, index) <- gmvPer.zipWithIndex) {
                if (gmv < gmvGrid) {
                    resultIndex = index
                    break()
                }
            }
        }
        resultIndex
    }
}
