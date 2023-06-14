package waimai.job.remote.flashbuy.yx

import waimai.utils.SparkJobs.RemoteSparkJob
import waimai.utils.JsonUtils.jsonObjectStrToArrayMap
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import scala.util.control.Breaks._

object metrics extends RemoteSparkJob {

    override def run(): Unit = {
        val beginDt = params.beginDt
        val endDt = params.endDt
        val mv = spark.sql(
            s"""
               |select ad_request_id,
               |       hour,
               |       poi_id,
               |       act,
               |       is_charge,
               |       final_charge,
               |       sub_ord_num,
               |       sub_mt_charge_fee,
               |       sub_total,
               |       cast(get_json_object(substr(ad_result_list, 2, length(ad_result_list)-2), '$$.pctr') as double) as metric
               |  from mart_waimaiad.pt_newpage_dsa_ad_mpv
               |  where dt between $beginDt and $endDt
               |  and get_json_object(substr(ad_result_list, 2, length(ad_result_list)-2), '$$.poi_id') = cast(poi_id as string)
               |""".stripMargin).as[Request].rdd.cache()

        val ctrThreshold = mv.map{ request => (request.poi_id, request) }.groupByKey.map{
            case (poi_id, iter) =>
                val tmp = iter.toList.sortBy(_.metric)
                val checkpoint = tmp.size / 10     // 551 / 10 => 55
                var view_num = tmp.count(x => x.act == 3)
                var final_charge = tmp.filter(x => x.is_charge == 1).map(_.final_charge).sum
                var i = 0
                breakable {
                    while (i < tmp.size) {
                        if ( i % checkpoint == 0) {
                            if (check(view_num, final_charge)) {
                                break()
                            }
                        }
                        if (tmp(i).act == 3) {
                            view_num -= 1
                        }
                        if (tmp(i).is_charge == 1){
                            final_charge -= tmp(i).final_charge
                        }
                        i += 1
                    }
                }
                val ctr = {
                    if (i == tmp.size) {
                        tmp.takeRight(1).head.metric
                    } else {
                        tmp(i).metric
                    }
                }
                (poi_id, ctr)
        }


        val x = mv.map{request => (request.poi_id, request)}.join(ctrThreshold).filter{
            case (poi_id, (request, ctr)) =>
                request.metric >= ctr
        }.map{ case (poi, (Request(ad_request_id, hour, poi_id, act, is_charge, final_charge, sub_order_num, sub_total, sub_mt_charge_fee, metric), ctr)) =>
            val view_num = if (act == 3) 1 else 0
            val click_num = if (act == 2) 1 else 0
            val charge = if (is_charge == 1) final_charge else 0
            val order_num = sub_order_num
            val price = sub_total + sub_mt_charge_fee
            (view_num, click_num, charge, order_num, price)
        }.reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5))


        println(s"${x._1}, ${x._2}, ${x._3}, ${x._4}, ${x._5}")

    }

    def check(view_num: Int, final_charge: Double, threshold: Double = 45.0): Boolean = {
        final_charge / view_num * 1000 >= threshold
    }
}
