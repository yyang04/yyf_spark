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
               |       get_json_object(substr(ad_result_list, 2, length(ad_result_list)-2), '$$.bid_ocpm') as gmv
               |  from mart_waimaiad.pt_newpage_dsa_ad_mpv
               |  where dt between $beginDt and $endDt
               |  and get_json_object(substr(ad_result_list, 2, length(ad_result_list)-2), '$$.poi_id') = cast(poi_id as string)
               |""".stripMargin).as[Request].rdd.cache()

        val per = mv.filter { request => request.act == 3 }.map{ request => (request.hour, request) }.groupByKey.map{
            case (hour, iter) =>
                val gmvPerHour = iter.map { request => request.ptgmv }
                val percentile = new Percentile
                val gmvPer = Range.inclusive(10, 90, 10).toArray.map{ x => percentile.evaluate(gmvPerHour.toArray, x.toDouble) }
                (hour.toInt, gmvPer)
        }.collect.sortBy(_._1)

        per.foreach{ case (hour, gmvPer) =>
            println(s"Hour:$hour, ${gmvPer.mkString(",")}")
        }

        mv.map { request => (grade(request.hour.toInt, request.ptgmv, per), request)
        }.groupByKey.map{
            case (grade, iter) =>
                val result = iter.map{ case Request(ad_request_id, hour, poi_id, act, is_charge, final_charge, sub_order_num, sub_total, sub_mt_charge_fee, ptgmv) =>
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
        var resultIndex = gmvPerHour(0)._2.length
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
