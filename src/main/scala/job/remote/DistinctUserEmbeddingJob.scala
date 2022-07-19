package job.remote

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import utils.SparkJobs.RemoteSparkJob

object DistinctUserEmbeddingJob extends RemoteSparkJob{
    override def run(): Unit = {
        val path = "viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/afo_model_newpage_life/20220718_190358/"
        val file = sc.textFile(path + "user_embedding")
        val data = file.map(row => {
            val expose_time = row.split(',')(0).toDouble.toLong
            val uuid = row.split(',')(1)
            val user_emb = row.split(',').slice(2, row.length).map(_.toDouble)
            (uuid, (expose_time, user_emb))
        }).reduceByKey((x, y) => if (x._1 > y._1) x else y ).map{
            case(uuid, (expose_time, user_emb)) => (uuid, user_emb)
        }.repartition(8000)
        saveAsTable(data, "mart_waimaiad.privacy_clustering_user_emb_test", "20211125")
    }

    def saveAsTable(res: RDD[(String, Array[Double])],
                    tableName: String,
                    date: String): DataFrame = {

        spark.sql(s"""
                create table if not exists $tableName (
                    uuid string,
                    user_emb array<double>
                ) partitioned by (dt string)
                STORED AS ORC
            """.stripMargin)
        val temp_input_data = "temp_input_data"
        val df = res.toDF("uuid", "user_emb")

        df.createOrReplaceTempView(temp_input_data)
        spark.sql(s"""
                insert overwrite table $tableName partition (dt=$date)
                select * from (
                    $temp_input_data
            )""".stripMargin)
    }
}
