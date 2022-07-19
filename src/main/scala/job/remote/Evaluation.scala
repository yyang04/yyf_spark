package job.remote

import utils.SparkJobs.RemoteSparkJob
import utils.privacy_clustering.Metrics

object Evaluation extends RemoteSparkJob{
    override def run(): Unit = {
        val data = spark.sql(
            """select uuid, user_emb, cluster_center
              |from mart_waimaiad.privacy_clustering_user_cluster_test
              |where dt = 20211125
              |and cluster = simhash
              |""".stripMargin).rdd.map(row=>{
            val uuid = row.getAs[String](0)
            val user_emb = row.getAs[Seq[Double]](1).toArray
            val cluster_center = row.getAs[Seq[Double]](2).toArray
            (user_emb, cluster_center)
        })
        println("cosine similarity", Metrics.evaluate_cosine(data))
        println("minkowski distance", Metrics.evaluate_minkowski(data))
    }
}
