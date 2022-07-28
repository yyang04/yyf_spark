package job.remote.privacy

import utils.SparkJobs.RemoteSparkJob
import utils.TimeOperations
import utils.SparkJobs.FileOperations

object DistinctUserEmbeddingJob extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.beginDt
        val timestamp = params.timestamp

        val ts = TimeOperations.getTimestamp(dt)
        val basePath = "viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/"
        val extraPath = s"yangyufeng04/bigmodel/afo_model_newpage_life/$timestamp"
        val path = basePath + extraPath + "/user_embedding"
        val file = sc.textFile(path)

        val data = file.map(row => {
            val expose_time = row.split(',')(0).toDouble.toLong
            val uuid = row.split(',')(1)
            val user_emb = row.split(',').slice(2, row.length).map(_.toDouble)
            (uuid, (expose_time, user_emb))
        })
          .filter(x => x._2._1 < ts)
          .reduceByKey((x, y) => if (x._1 > y._1) x else y)
          .map { case (uuid, (expose_time, user_emb)) => (uuid, user_emb) }
          .repartition(8000)

        val df = data.toDF("uuid", "user_emb")
        FileOperations.saveAsTable(spark, df, "user_emb_floc_test", Map("dt" -> dt))
    }
}
