package waimai.job.remote.flashbuy.metrics

import waimai.utils.SparkJobs.RemoteSparkJob

import scala.collection.mutable
import scala.concurrent.duration._

case class ActivityDayMetricEntity (uuid: String,
                                    sku_id: Long,
                                    event_timestamp: Long,
                                    upc_code: String)


object ActivityDayMetric extends RemoteSparkJob {

    override def run(): Unit = {
        val dt = params.dt
        val result = spark.sql(
            s"""
               | select uuid,
               |        sku_id,
               |        event_timestamp,
               |        upc_code
               |   from mart_lingshou.fact_flow_sdk_product_mv mv
               |   join
               |    ( select product_id,
               |             upc_code
               |        from mart_lingshou.dim_prod_product_sku_s_snapshot
               |        where dt=$dt
               |          and upc_code is not null
               |    ) info
               |    on mv.sku_id=info.product_id
               |  where dt=$dt
               |    and event_type='view'
               |    and uuid is not null
               |    and sku_id is not null
               |""".stripMargin).as[ActivityDayMetricEntity]
          .rdd
          .map{ x => (x.uuid, x) }
          .groupByKey
          .map{
              case (uuid, iter) =>
                  val itemSeq = iter.toList.sortBy(_.event_timestamp)
                  val dailyBehavior = new mutable.ListBuffer[mutable.ListBuffer[ActivityDayMetricEntity]]
                  val session = new mutable.ListBuffer[ActivityDayMetricEntity]

                  var lastTimeStamp = 0L
                  for (i <- itemSeq.indices) {
                      if ((itemSeq(i).event_timestamp - lastTimeStamp >= 30.minute.toMillis) && session.nonEmpty) {
                          dailyBehavior.append(session)
                          session.clear()
                      }
                      session.append(itemSeq(i))
                      lastTimeStamp = itemSeq(i).event_timestamp
                  }

                  var count = 0
                  for (sess <- dailyBehavior) {
                      val upcCode = sess.groupBy(_.upc_code).mapValues(iter => iter.toList.map(_.sku_id).distinct.size)
                      if (upcCode.values.exists(_ >= 2)){
                          count += 1
                      }
                  }
                  (count, 1)
        }.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        println(result._1, result._2)
    }

}
