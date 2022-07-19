package job.remote

import utils.SparkJobs.RemoteSparkJob
object SimHashJob extends RemoteSparkJob {
    override def run(): Unit = {
        val path = "viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/afo_model_newpage_life/20220718_190358/"
        val file = sc.textFile(path + "user_embedding_unique")


    }
}
