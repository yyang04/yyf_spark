package waimai.job.remote.flashbuy.recall.v2i.sample

case class ModelSample (event_type: String= "",
                        request_id: String= "",
                        uuid: String= "",
                        user_id: Option[String]= Some(""),
                        sku_id: Long=0L,
                        spu_id: Option[Long]=Some(0L),
                        poi_id: Long=0L)
