package waimai.job.remote.privacy

import waimai.utils.FileOperations.saveAsTable
import waimai.utils.PrivacyClustering.PrefixLshClustering
import waimai.utils.SparkJobs.RemoteSparkJob

object PrefixLshJob extends RemoteSparkJob {
    // prefixlsh
    // (cosine similarity,0.7571927584345486)
    // (minkowski distance,3.562684651420528)

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
        saveAsTable(spark, result, "privacy_clustering_test", Map("dt" -> dt, "algorithm" -> "prefixlsh", "threshold" -> threshold))
    }
}
