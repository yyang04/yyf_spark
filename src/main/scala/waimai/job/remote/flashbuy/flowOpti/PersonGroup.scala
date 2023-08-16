package waimai.job.remote.flashbuy.flowOpti

import com.taobao.tair3.client.TairClient
import utils.TairUtil
import waimai.utils.FileOp
import waimai.utils.SparkJobs.RemoteSparkJob
import waimai.utils.DateOp.{getNDaysAgo, getNDaysAgoFrom, getTsForNextWeek}

import scala.collection.JavaConverters._
import scala.concurrent.duration._


object PersonGroup extends RemoteSparkJob {
    val prefix = "PtSgOneStopTrigger_"
    override def run(): Unit = {
        val window = params.window match { case 0 => 30 ; case x: Int => x }
        val prod = params.mode match { case "" => true; case _: String => false }
        val endDt = params.endDt match { case "" => getNDaysAgo(1); case x: String => x}

        val beginDt = getNDaysAgoFrom(endDt, window)
        val expireTs = getTsForNextWeek

        val rdd = spark.sql(
            s"""
               |select uuid
               |  from mart_waimaiad.pt_sg_uuid_click_daily
               | where dt between $beginDt and $endDt
               | group by 1
               |""".stripMargin).rdd.map{ row =>
            val uuid = row.getAs[String](0)
            uuid
        }.cache
        FileOp.saveAsTable(rdd.toDF("uuid"), "pt_sg_uuid_click_flow_opti", partition=Map("dt" -> endDt, "window" -> window))
        if (prod) {
            val data = rdd.collect().map{x => (x, "1")}
            saveTair(data, expireTs)
        }
    }

    def saveTair(data: Array[(String, String)], expireTs: Int, step: Int = 50000): Unit = {
        val client = new TairUtil
        val tairOption = new TairClient.TairOption(5000, 0, expireTs)
        data.grouped(step).foreach { x =>
            val inputData = x.map { case (k, v) =>
                println(prefix + k, v)
                (prefix + k, v)
            }.toMap.asJava
            client.batchPutString(inputData, 4, tairOption)
            Thread.sleep(10.seconds.toMillis)
        }
    }

}
