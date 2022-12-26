package job.remote.flashbuy.u2i

import job.remote.flashbuy.u2i.U2IInfer.{hdfs, read_raw, sc}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import play.api.libs.json.Json
import utils.{FileOperations, JSONUtils, S3Handler}
import utils.SparkJobs.RemoteSparkJob

import java.io.{File, PrintWriter}

object SkuIndex extends RemoteSparkJob{

    override def run(): Unit = {
        val dt = params.dt
        val ts = params.timestamp
        val bucket = "com-sankuai-wmadrecall-hangu-admultirecall"
        val bucketTableName = "ptU2ISkuEmb"
        val sku_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$ts/sku_embedding/$dt"

        if (!FileOperations.waitUntilFileExist(hdfs, sku_path)) { sc.stop(); return}
        println("123")

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
            val spu_id = row.getLong(2)
            (sku_id, (poi_id, spu_id))
        }.join(sku).map { case (sku, ((poi, spu_id), emb)) =>
            Json.obj(
                "embV1" -> Json.arr(),
                "embV2" -> Json.arr(emb),
                "geoHash" -> " ",
                "poiId" -> poi.toInt,
                "skuId" -> sku.toInt,
                "skuId4R" -> sku.toInt,
                "spuId" -> spu_id.toInt,
                "spuName" -> " "
            ).toString()
        }

        write(poi_sku, bucket, bucketTableName, "v1")
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
        }
    }
}
