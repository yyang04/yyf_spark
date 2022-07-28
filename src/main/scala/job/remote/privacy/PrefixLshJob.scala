package job.remote.privacy

import utils.SparkJobs.RemoteSparkJob
import utils.PrivacyClustering.PrefixLshClustering
import utils.FileOperations.saveAsTable

object PrefixLshJob extends RemoteSparkJob {
    override def run(): Unit = {
        val threshold = params.threshold
        val dt = params.beginDt
        val data = spark.sql(
            s"""select uuid, user_emb
              |from mart_waimaiad.yyf04_user_emb_floc_test
              |where dt = $dt
              |""".stripMargin).rdd.map(row => {
            val uuid = row.getAs[String](0)
            val user_emb = row.getAs[Seq[Double]](1).toArray
            (uuid, user_emb)
        })
        val model = new PrefixLshClustering(20, 10, threshold)
        val result = model.fit(data).toDF("uuid", "user_emb", "cluster_center")
        saveAsTable(spark, result, "clustering_test", Map("dt" -> dt, "algorithm" -> "prefixlsh", "threshold" -> threshold))
    }
}
