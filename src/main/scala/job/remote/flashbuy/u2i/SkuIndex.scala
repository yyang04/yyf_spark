package job.remote.flashbuy.u2i

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import play.api.libs.json.{JsArray, JsNumber, Json}
import utils.{FileOperations, JSONUtils, S3Handler}
import utils.SparkJobs.RemoteSparkJob

import java.time.LocalDateTime
import java.io.{File, PrintWriter}
import java.time.format.DateTimeFormatter
import java.util.Date

object SkuIndex extends RemoteSparkJob{

    override def run(): Unit = {
        val dt = params.dt
        val ts = params.timestamp
        val bucket = "com-sankuai-wmadrecall-hangu-admultirecall"
        val bucketTableName = "ptU2ISkuEmb"
        val sku_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$ts/sku_embedding/$dt"
        // viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/20221223_154733/sku_embedding/20221222

        if (!FileOperations.waitUntilFileExist(hdfs, sku_path)) { sc.stop(); return }

        val tableName = "PtVectorSg"
        val timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
        val utime = new Date().getTime

        val sku = read_raw(sc, sku_path)

        val poi_sku = spark.sql(
            s"""
               |select cast(sku_id as string) as sku_id,
               |       poi_id,
               |       product_spu_id
               |  from mart_waimaiad.recsys_linshou_pt_poi_skus
               | where dt='$dt'
               |""".stripMargin).rdd.map { row =>
            val sku_id = row.getString(0)
            val poi_id = row.getLong(1)
            (sku_id, poi_id)
        }.join(sku).map { case (sku_id, (poi, emb)) =>
            val data = Json.obj(
                "embV1" -> JsArray(emb.map(JsNumber(_))),
                "poiId" -> poi,
                "skuId" -> sku_id.toLong,
            ).toString
            Json.obj(
                "table" -> tableName,
                "utime" -> utime,
                "data" -> data,
                "opType" -> 1,
                "pKey" -> sku_id
            ).toString
        }

        write(poi_sku, bucket, bucketTableName, timestamp)
    }

    def write(s: RDD[String], bucket: String, bucketTableName: String, version: String): Unit = {
        s.repartition(100).mapPartitions { x =>
            val idx = TaskContext.getPartitionId()
            val filePath = "tmp" + File.separator + "index"
            val writer = new PrintWriter(filePath)
            x.foreach(e => {
                writer.println(e)
            })
            writer.close()
            val file = new File(filePath)
            println(s"file length: ${file.length()}")
            S3Handler.putObjectFile(filePath, bucket, s"$bucketTableName/$version/part-$idx")
            file.delete()
            x
        }.count()
    }

    def read_raw(sc: SparkContext, path: String): RDD[(String, Array[Float])] = {
        sc.textFile(path).map { row =>
            val id = row.split(",")(0)
            val emb = row.split(",").drop(1).map(_.toFloat)
            (id, emb)
        }
    }
}
