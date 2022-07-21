package job.remote.privacy

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import utils.SparkJobs.RemoteSparkJob
import utils.privacy_clustering.SimHashClustering

object SimHashJob extends RemoteSparkJob {

    // simhash
    // (cosine similarity,0.6773179399884419)
    // (minkowski distance,3.750147692362917)

    // prefixlsh
    // (cosine similarity,0.7571927584345486)
    // (minkowski distance,3.562684651420528)


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
        val model = new SimHashClustering(18, 1, threshold)
        val result = model.fit(data)
        saveAsTable(rdd = result,
            tableName = "mart_waimaiad.privacy_clustering_user_cluster_test",
            date = "20211125",
            threshold = threshold,
            algorithm = "simhash")
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
