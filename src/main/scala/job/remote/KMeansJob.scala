package job.remote
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import utils.SparkJobs.RemoteSparkJob

object KMeansJob extends RemoteSparkJob {
    override def run(): Unit = {
        val data = spark.sql(
            """select uuid, user_emb
              |from mart_waimaiad.privacy_clustering_user_emb_test
              |where dt = 20211125
              |""".stripMargin).rdd.map(row=>{
            val uuid = row.getAs[String](0)
            val user_emb = row.getAs[Seq[Double]](1).toArray
            (uuid, Vectors.dense(user_emb))
        }).cache()

        









    }
}
