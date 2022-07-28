package job.remote.privacy

import utils.PrivacyClustering.{AffinityClusteringLocal, PrefixLshClustering}
import utils.FileOperations.saveAsTable
import utils.SparkJobs.RemoteSparkJob

object AffinityClusteringSampleJob extends RemoteSparkJob{
    override def run(): Unit = {
        val threshold = params.threshold
        val dt = params.beginDt
        val data = spark.sql(
            s"""select uuid, user_emb
               |from mart_waimaiad.privacy_clustering_user_emb_test
               |where dt = $dt
               |""".stripMargin).rdd.map(row => {
            val uuid = row.getAs[String](0)
            val user_emb = row.getAs[Seq[Double]](1).toArray
            (uuid, user_emb)
        })

        val model = new AffinityClusteringLocal(
            upperBound = 2000,
            lowerBound = 40,
            threshold = 2000,
            numHashes = 300,
            signatureLength = 15,
            joinParallelism = 4000,
            bucketLimit = 1000,
            bucketWidth = 20,
            outputPartitions = 4000,
            num_neighbors = 1,
            sample_fraction = 0.1
        )

        val result = model.fit(sc, data).toDF("uuid", "user_emb", "cluster_center")
        saveAsTable(spark, result, "clustering_test", Map("dt" -> dt, "algorithm" -> "affinityclustering", "threshold" -> threshold))
    }
}
