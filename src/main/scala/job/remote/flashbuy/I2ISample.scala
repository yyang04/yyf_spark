package job.remote.flashbuy

import utils.SparkJobs.RemoteSparkJob

object I2ISample extends RemoteSparkJob{
    override def run(): Unit = {

        val geoWithCity = spark.sql(
            s"""
               |select distinct poi_geohash, aor_id
               |  from mart_waimaiad.recsys_linshou_user_explicit_acts
               | where dt='20220202'
               |   and aor_id is not null and aor_id > 0
               |""".stripMargin).rdd.map(x => (x.getString(0), x.getInt(1))).collectAsMap().toMap
        val scGeo = sc.broadcast(geoWithCity)






    }
}
