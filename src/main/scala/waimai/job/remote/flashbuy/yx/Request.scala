package waimai.job.remote.flashbuy.yx

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
                   metric: Double
                  )
