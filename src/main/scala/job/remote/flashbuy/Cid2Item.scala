package job.remote.flashbuy

import utils.SparkJobs.RemoteSparkJob

object Cid2Item extends RemoteSparkJob {
    override def run(): Unit = {
        spark.sql("CREATE TEMPORARY FUNCTION mt_geohash AS 'com.sankuai.meituan.hive.udf.UDFGeoHash'")







    }
}
