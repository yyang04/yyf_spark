package job.remote

import utils.SparkJobs.RemoteSparkJob

object DistinctUserEmbeddingJob extends RemoteSparkJob{
    override def run(): Unit = {
        val path = "viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/afo_model_newpage_life/20220718_190358/"
        val file = sc.textFile(path + "user_embedding")
        val data = file.map(row => {
            val expose_time = row.split(',')(0).toLong
            val uuid = row.split(',')(1)
            val user_emb = row.split(',').slice(2, row.length).map(_.toDouble)
            (uuid, (expose_time, user_emb))
        }).reduceByKey((x, y) => if (x._1 > y._1) x else y ).repartition(8000)
        data.saveAsTextFile(path+ "user_embedding_unique")
    }
}
