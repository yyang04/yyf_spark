package waimai.job.remote.flashbuy.s2i

import waimai.utils.SparkJobs.RemoteSparkJob

object AoiSearchLog extends RemoteSparkJob {
    override def run(): Unit = {
        val reader = AoiUtil.getAoiInstance
        val bcReader = sc.broadcast(reader)







    }

}
