package waimai.job.remote.flashbuy.metrics

import waimai.utils.SparkJobs.RemoteSparkJob
import scala.collection.mutable
import scala.concurrent.duration._

case class ActivityDayMetricEntity (dt: String,
                                    uuid: String,
                                    sku_id: Long,
                                    event_timestamp: Long,
                                    upc_code: String,
                                    event_id: String,
                                    page_id: String)


object ActivityDayMetric extends RemoteSparkJob {

    override def run(): Unit = {
        val beginDt = params.beginDt
        val endDt = params.endDt
        val threshold = params.threshold
        val result = spark.sql(
            s"""
               | select mv.dt,
               |        uuid,
               |        sku_id,
               |        event_timestamp,
               |        upc_code,
               |        event_id,
               |        page_id
               |   from mart_lingshou.fact_flow_sdk_product_mv mv
               |   join
               |    ( select dt,
			   |             product_id,
               |             upc_code
               |        from mart_lingshou.dim_prod_product_sku_s_snapshot
               |        where dt between $beginDt and $endDt
               |          and upc_code is not null
               |    ) info
               |    on mv.sku_id=info.product_id and mv.dt=info.dt
               |  where mv.dt between $beginDt and $endDt
               |    and event_type='view'
               |    and uuid is not null
               |    and sku_id is not null
               |""".stripMargin).as[ActivityDayMetricEntity]
          .rdd
          .map{ x => ((x.dt, x.uuid), x) }
          .groupByKey
          .map{
              case (key, iter) =>
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
                  dailyBehavior.append(session)

                  var count = 0
                  for (sess <- dailyBehavior) {
                      val upcCode = sess.groupBy(_.upc_code).mapValues(_.toList.map(_.sku_id).distinct.size)
                      if (upcCode.values.exists(_ >= threshold)){
                          count += 1
                      }
                  }
                  (key._1, (count, dailyBehavior.size, 1)) }
          .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
          .collect

        result.sortBy(_._1).foreach{ case (dt, (seqCount, totalSize, totalUser)) =>
            println(dt, seqCount, totalSize, totalUser)
        }
    }

}
