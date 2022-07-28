package job.remote.privacy

import utils.SparkJobs.RemoteSparkJob
import utils.PrivacyClustering.SimHashClustering
import utils.FileOperations.saveAsTable

object SimHashJob extends RemoteSparkJob {
    // simhash
    // (cosine similarity,0.6773179399884419)
    // (minkowski distance,3.750147692362917)

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

        val model = new SimHashClustering(18, 1, threshold)
        val result = model.fit(data).toDF("uuid", "user_emb", "cluster_center")
        saveAsTable(spark, result, "privacy_clustering_test", Map("dt" -> dt, "algorithm" -> "simhash", "threshold" -> threshold))
    }
}
