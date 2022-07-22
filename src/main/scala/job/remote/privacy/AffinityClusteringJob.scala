package job.remote.privacy

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import utils.SparkJobs.RemoteSparkJob
import utils.privacy_clustering.AffinityClustering

object AffinityClusteringJob extends RemoteSparkJob {
    override def run(): Unit = {
        val threshold = params.threshold
        val data = spark.sql(
            """select uuid, user_emb
              |from mart_waimaiad.privacy_clustering_user_emb_test
              |where dt = 20211125
              |limit 1000000
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
        val result = model.fit(sc, data)

        saveAsTable(rdd = result,
            tableName = "mart_waimaiad.privacy_clustering_user_cluster_test",
            date = "20211125",
            threshold = threshold,
            algorithm = "AffinityClustering")
    }

    def saveAsTable(rdd: RDD[(String, Array[Double], Array[Double])],
                    tableName: String,
                    date: String,
                    threshold: Int,
                    algorithm: String): DataFrame = {

        spark.sql(
            s"""
                create table if not exists $tableName (
                    uuid string,
                    user_emb array<double>,
                    cluster_center array<double>
                ) partitioned by (dt string, threshold int, algorithm string)
                STORED AS ORC
            """.stripMargin)

        val temp_input_data = "temp_input_data"
        val df = rdd.toDF("uuid", "user_emb", "cluster_center")
        df.createOrReplaceTempView(temp_input_data)
        spark.sql(
            s"""
                insert overwrite table $tableName partition (dt=$date, threshold=$threshold, algorithm='$algorithm')
                select * from (
                    $temp_input_data
            )""".stripMargin)
    }
}
