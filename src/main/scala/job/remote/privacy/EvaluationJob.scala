package job.remote.privacy

import utils.SparkJobs.RemoteSparkJob
import utils.PrivacyClustering.ClusteringMetrics
import scala.collection.mutable

object EvaluationJob extends RemoteSparkJob {
    override def run(): Unit = {
        val algorithm = params.algorithm
        val data = spark.sql(
            s"""select uuid, user_emb, cluster_center
               |from mart_waimaiad.yyf04_clustering_test
               |where dt = 20211125
               |and threshold = 2000
               |and algorithm = '$algorithm'
               |""".stripMargin).rdd.map(row => {
            val user_emb = row.getAs[Seq[Double]](1).toArray
            val cluster_center = row.getAs[Seq[Double]](2).toArray
            (user_emb, cluster_center)
        })
        println("cosine similarity", ClusteringMetrics.evaluate_cosine(data))
        println("minkowski distance", ClusteringMetrics.evaluate_minkowski(data))

        val nodeRelation = spark.sql("""""").rdd.map(row => {
            val poi_id: String = row.getAs[String]("poi_id")
            val geohash_code: String = row.getAs[String]("geohash_code")
            val spu_id: String = row.getAs[String]("spu_id")
            val spu_name: String = row.getAs[String]("spu_name")
            val bidWordsFilter: mutable.Map[String, Float] = mutable.Map()
            row.getAs[Map[String, Float]]("bidwords").filter(input => input._2 > 0.9).foreach(
                input => {
                    bidWordsFilter += input._1 -> input._2
                }
            );bidWordsFilter
        })

    }

}
