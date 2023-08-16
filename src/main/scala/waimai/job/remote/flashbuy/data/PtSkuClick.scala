package waimai.job.remote.flashbuy.data

import waimai.utils.DateOp.getNDaysAgo
import waimai.utils.SparkJobs.RemoteSparkJob

object PtSkuClick extends RemoteSparkJob {


    override def run(): Unit = {

        val dt = params.dt match {
            case "" => getNDaysAgo(1)
            case x => x
        }

        spark.sql(
            s"""
               |               select mv.dt,
               |                      mv.ad_request_id,
               |                      mv.act,
               |                      mv.expose_num,
               |                      mv.ad_bid_amt,
               |                      mv.is_charge,
               |                      mv.final_charge,
               |                      mv.click_num,
               |                      mv.attribute['spuIdList'] as spuIdList,
               |                      mv.attribute['spu_id'] as spu_id,
               |                      coalesce(od.sub_mt_charge_fee, 0.0) as sub_mt_charge_fee,
               |                      coalesce(od.sub_total, 0.0) as sub_total,
               |                      coalesce(od.sub_ord_num, 0) as sub_ord_num
               |                 from mart_waimai_dw_ad.fact_flow_ad_entry_mv mv
               |                 join (
               |                     select dt, poi_id
               |                       from mart_lingshou.aggr_poi_info_dd
               |                      where dt=$dt
               |                 ) info on mv.poi_id=info.poi_id and mv.dt=info.dt
               |                 left join (
               |                       select dt,
               |                              ad_request_id,
               |                              sub_mt_charge_fee,
               |                              sub_total,
               |                              sub_ord_num
               |                         from mart_waimai_dw_ad.fact_flow_ad_sdk_entry_order_view
               |                        where dt=$dt
               |                          and is_ad=1 and is_push=1
               |                 ) od on mv.ad_request_id=od.ad_request_id and mv.dt=od.dt and mv.act=2
               |                where mv.dt=$dt
               |                  and mv.is_valid = 'PASS'
               |                  and slot in (160, 162, 192, 193, 195)
               |""".stripMargin)






    }
}
