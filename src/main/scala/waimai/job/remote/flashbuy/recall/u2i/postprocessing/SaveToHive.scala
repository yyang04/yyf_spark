package waimai.job.remote.flashbuy.recall.u2i.postprocessing

import org.apache.spark.rdd.RDD
import waimai.utils.SparkJobs.RemoteSparkJob
import org.apache.spark.SparkContext

object SaveToHive extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.dt
        val version = params.version
        val userEmbeddingPath = params.src_table_name
        val itemEmbeddingPath = params.dst_table_name

        val partition = Map("dt" -> dt, "version" -> "")
    }

    def readUserEmbedding(sc: SparkContext, path: String): RDD[(String, String, Array[Float])] = {
        // request_id,uuid,embedding1,embedding2...
        val result = sc.textFile(path).map { row =>
            val splitPart = row.split(",")
            val request_id = splitPart(0)
            val uuid = splitPart(1)
            val emb = splitPart.drop(2).map(_.toFloat)
            (request_id, uuid, emb)
        }
        println(result.take(1).head)
        result
    }

    def readItemEmbedding(sc: SparkContext, path: String): RDD[(String, Array[Float])] = {
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
