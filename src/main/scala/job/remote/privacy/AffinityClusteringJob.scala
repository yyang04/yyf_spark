package job.remote.privacy


import utils.SparkJobs.RemoteSparkJob
import utils.PrivacyClustering.AffinityClustering
import utils.FileOperations.saveAsTable


object AffinityClusteringJob extends RemoteSparkJob {
    override def run(): Unit = {
        val threshold = params.threshold
        val data = spark.sql(
            """select uuid, user_emb
              |from mart_waimaiad.privacy_clustering_user_emb_test
              |where dt = 20211125
              |""".stripMargin).rdd.map(row => {
            val uuid = row.getAs[String](0)
            val user_emb = row.getAs[Seq[Double]](1).toArray
            (uuid, user_emb)
        })

        val model = new AffinityClustering(
            upperBound = 3000,
            lowerBound = 500,
            threshold = 1000,
            numHashes = 300,
            signatureLength = 40,
            joinParallelism = 8000,
            bucketLimit = 1000,
            bucketWidth = 10,
            outputPartitions = 8000,
            num_neighbors = 5,
            num_steps = 5
        )
//        val result = model.fit(sc, spark, hdfs, data)
//        val df = result.toDF("uuid", "user_emb", "cluster_center")
//
//        saveAsTable(
//            spark, df,
//            tableName="privacy_clustering_user_cluster_test",
//            partition=Map("dt"-> "20211125", "threshold"-> 2000, "algorithm"-> "affinityclustering"))
    }
}
