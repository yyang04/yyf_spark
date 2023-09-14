package waimai.job.remote.flashbuy.flowOpti

import com.taobao.tair3.client.TairClient
import waimai.utils.SparkJobs.RemoteSparkJob
import waimai.utils.JsonOp.{iterableToJsonObjectStr, jsonObjectStrToMap}

import scala.collection.JavaConverters._
import utils.TairUtil

import scala.util.control.Breaks._
import waimai.utils.DateOp.{getNDaysAgo, getNDaysAgoFrom}
import waimai.utils.FileOp

case class Request(ad_request_id: String,
                   slot:Int,
                   hour:String,
                   poi_id:Long,
                   act:Int,
                   is_charge:Int,
                   final_charge:Double,
                   sub_ord_num:Int,
                   sub_total:Double,
                   sub_mt_charge_fee:Double,
                   pctr:Double,
                   pgmv:Double
                  )

object OfflineMetrics extends RemoteSparkJob {

    override def run(): Unit = {
        val window = params.window                               // 时间窗口 设置为30吧
        val endDt = params.endDt
        val beginDt = getNDaysAgoFrom(endDt, window)
        val expName = params.expName                             // 实验名称: 例如 CTR_50 或者 GMV_20
        val threshold = expName.split("_").last.toInt
        val factor = expName.split("_").head             // 暂时只有CTR的，没有ROI的
        val mode = ""

        val mv = spark.sql(
            s"""
               |select ad_request_id,
               |       slot,
               |       hour,
               |       poi_id,
               |       act,
               |       is_charge,
               |       final_charge,
               |       sub_ord_num,
               |       sub_mt_charge_fee,
               |       sub_total,
               |       cast(get_json_object(substr(ad_result_list, 2, length(ad_result_list)-2), '$$.pctr') as double) as pctr,
			   |       cast(get_json_object(substr(ad_result_list, 2, length(ad_result_list)-2), '$$.ppt_gmv') as double) as pgmv
               |  from mart_waimaiad.pt_newpage_dsa_ad_mpv
               |  where dt between $beginDt and $endDt
               |    and get_json_object(substr(ad_result_list, 2, length(ad_result_list)-2), '$$.poi_id') = cast(poi_id as string)
               |""".stripMargin)
          .as[Request]
          .rdd
          .cache()

        val ctrThreshold = mv
          .map{ request => ((request.poi_id, request.slot), request) }
          .groupByKey
          .map{
            case ((poi_id, slot), iter) =>
                val tmp = iter.toList.sortBy(_.pctr)
                val checkpoint = scala.math.max(tmp.size / 100, 10)    // max(551/50, 10) => 11
                var view_num = tmp.count(x => x.act == 3)
                var final_charge = tmp.filter(x => x.is_charge == 1).map(_.final_charge).sum

                var i = 0
                breakable {
                    while (i < tmp.size) {
                        if ( i % checkpoint == 0) {
                            if (check(view_num, final_charge, threshold)) {
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
                        tmp.takeRight(1).head.pctr
                    } else {
                        tmp(i).pctr
                    }
                }
                (poi_id, slot, ctr)
        }.cache

        FileOp.saveAsTable(ctrThreshold.toDF("poi_id","slot", "ctr"), "pt_sg_dsa_ctr_threshold_common_slot", Map("dt" -> endDt, "threshold" -> threshold))

        val tairData = ctrThreshold.map{
              case (poi_id, slot, ctr) ⇒ (poi_id, (slot, ctr))
          }
          .groupByKey
          .map { case (poi_id, iter) ⇒
                  (poi_id, iter.toMap) }
          .collect
          .map{
            case (poi_id, ctrMap) =>
                val value = ctrMap.map{ case(k, v) ⇒ (expName + "_" + k.toString, v.toString) }
            ("PtSgAdFlowSmooth" + poi_id.toString, value)
        }

        saveTair(tairData)

        val ctr = ctrThreshold.map{ case (a, b, c) ⇒ ((a, b), c) }

        val x = mv.map{ request => ((request.poi_id, request.slot), request) }
          .join(ctr)
          .filter{
            case (poi_id, (request, ctr)) =>
                request.pctr >= ctr
        }.map{ case (x, (Request(ad_request_id, slot, hour, poi_id, act, is_charge, final_charge, sub_order_num, sub_total, sub_mt_charge_fee, pctr, pgmv), ctr)) =>
            val view_num = if (act == 3) 1 else 0
            val click_num = if (act == 2) 1 else 0
            val charge = if (is_charge == 1) final_charge else 0
            val order_num = sub_order_num
            val price = sub_total + sub_mt_charge_fee
            (view_num, click_num, charge, order_num, price)
        }.reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5))
        println(s"${x._1}, ${x._2}, ${x._3}, ${x._4}, ${x._5}")
    }

    def check(view_num: Int, final_charge: Double, threshold: Int): Boolean = {
        final_charge / view_num * 1000 >= threshold
    }

    def saveTair(data: Array[(String, Map[String, String])], step: Int = 500) : Unit = {
        val client = new TairUtil
        val tairOption = new TairClient.TairOption(5000)
        data.grouped(step).foreach{ x =>
            val queryResult = client.batchGetString(x.map(_._1).toBuffer.asJava, 4, 5000).asScala
            val parseQueryResult = queryResult.map {
                case (k, v) =>
                    val valueMap = jsonObjectStrToMap[String](v)
                    (k, valueMap.filter{ case (k, v) => !k.startsWith("ctr_5019") } )
            }

            val inputData = x.map{ case (k, v) =>
                val lastMap = parseQueryResult.getOrElse(k, Map())
                val updateMap = lastMap ++ v
                val oldStr = iterableToJsonObjectStr(lastMap)
                val updateStr = iterableToJsonObjectStr(updateMap)
                println(k, oldStr, updateStr)
                (k, updateStr)
            }.toMap.asJava

            client.batchPutString(inputData, 4, tairOption)
        }
    }

}
