package job.remote.privacy

import utils.SparkJobs.RemoteSparkJob
import utils.privacy_clustering.Metrics

object EvaluationJob extends RemoteSparkJob {
    override def run(): Unit = {
        val algorithm = params.algorithm
        val data = spark.sql(
            s"""select uuid, user_emb, cluster_center
               |from mart_waimaiad.privacy_clustering_user_cluster_test
               |where dt = 20211125
               |and threshold = 2000
               |and algorithm = '$algorithm'
               |""".stripMargin).rdd.map(row => {
            val user_emb = row.getAs[Seq[Double]](1).toArray
            val cluster_center = row.getAs[Seq[Double]](2).toArray
            (user_emb, cluster_center)
        })
        println("cosine similarity", Metrics.evaluate_cosine(data))
        println("minkowski distance", Metrics.evaluate_minkowski(data))
    }
}
