package waimai.job.remote.flashbuy.recall.u2i.postprocessing

import org.apache.spark.rdd.RDD
import waimai.utils.SparkJobs.RemoteSparkJob
import org.apache.spark.SparkContext
import waimai.utils.FileOp.saveAsTable

object SaveToHive extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.dt
        val version = params.version
        val userEmbeddingPath = params.src_table_name
        val itemEmbeddingPath = params.dst_table_name
        val userEmbedding = readEmbedding(sc, userEmbeddingPath)
        val itemEmbedding = readEmbedding(sc, itemEmbeddingPath)

        val userDF = userEmbedding.toDF("key", "value")
        val userPartition = Map("dt" -> dt, "version" -> version, "entity" -> "user")
        val itemDF = itemEmbedding.toDF("key", "value")
        val itemPartition = Map("dt" -> dt, "version" -> version, "entity" -> "item")

        saveAsTable(spark, userDF, "pt_multi_recall_results_vector", partition=userPartition)
        saveAsTable(spark, itemDF, "pt_multi_recall_results_vector", partition=itemPartition)
    }

    def readEmbedding(sc: SparkContext, path: String): RDD[(String, Array[Float])] = {
        // request_id_uuid,embedding1,embedding2...
        // poi_id,embedding1,embedding2
        val result = sc.textFile(path).map { row =>
            val splitPart = row.split(",")
            val poi_id = splitPart(0)
            val emb = splitPart.drop(1).map(_.toFloat)
            (poi_id, emb)
        }
        println(result.take(1).head)
        result
    }
}
