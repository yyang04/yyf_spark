package waimai.job.remote.flashbuy.recall.u2i.postprocessing

import org.apache.spark.rdd.RDD
import waimai.utils.SparkJobs.RemoteSparkJob
import org.apache.spark.SparkContext
import waimai.utils.FileOp
import waimai.utils.FileOp.saveAsTable

object SaveToHive extends RemoteSparkJob {
    override def run(): Unit = {
        // 如果等待
        val dt = params.dt            // 存入表中partition的日期
        val version = params.version  // embedding的version
        val userEmbeddingPath = params.user_embedding_path  // user的hdfs地址
        val itemEmbeddingPath = params.item_embedding_path  // item的hdfs地址
        val hourWait = params.hour    // 等待的小时数,如果在等待的时间内，没有检测到hdfs下有数据则任务停止

        val userEmbedding = readEmbedding(sc, userEmbeddingPath, hourWait)
        val itemEmbedding = readEmbedding(sc, itemEmbeddingPath, hourWait)
        val userDF = userEmbedding match {
            case Some(x) => x.toDF("key", "value")
            case _ => return
        }
        val itemDF = itemEmbedding match {
            case Some(x) => x.toDF("key", "value")
            case _ => return
        }

        val userPartition = Map("dt" -> dt, "version" -> version, "entity" -> "user")
        val itemPartition = Map("dt" -> dt, "version" -> version, "entity" -> "item")
        saveAsTable(spark, userDF, "pt_multi_recall_results_vector", partition=userPartition)
        saveAsTable(spark, itemDF, "pt_multi_recall_results_vector", partition=itemPartition)
    }

    private def readEmbedding(sc: SparkContext, path: String, hourWait: Int): Option[RDD[(String, Array[Float])]] = {
        // request_id_uuid,embedding1,embedding2...
        // poi_id,embedding1,embedding2
        if (!FileOp.waitUntilFileExist(hdfs, path, hourWait=hourWait, minuteStep=5)) {
            sc.stop(); return None
        }
        val result = sc.textFile(path).map { row =>
            val splitPart = row.split(",")
            val poi_id = splitPart(0)
            val emb = splitPart.drop(1).map(_.toFloat)
            (poi_id, emb)
        }.repartition(100)
        println(result.take(1).head)
        Some(result)
    }
}
