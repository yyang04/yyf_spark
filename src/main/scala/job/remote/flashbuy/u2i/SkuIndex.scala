package job.remote.flashbuy.u2i

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import play.api.libs.json.{JsArray, JsNumber, Json}
import scalaj.http.Http
import utils.{FileOperations, JSONUtils, S3Handler}
import utils.SparkJobs.RemoteSparkJob

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Date
import utils.S3Connect

object SkuIndex extends RemoteSparkJob with S3Connect {

    override def run(): Unit = {
        initConnect(sc)
        // 将sku embedding上传到s3，并加载到函谷
        val dt = params.dt           // 哪一天的 sku embedding
        val ts = params.timestamp    // 模型的时间戳
        val mode = params.mode       // prod or stage

        val tableName = "PtVectorSg"
        val timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
        val utime = new Date().getTime
        val bucket = "com-sankuai-wmadrecall-hangu-admultirecall"
        val bucketTableName = "ptU2ISkuEmb"

        val sku_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$ts/sku_embedding/$dt"

        if (!FileOperations.waitUntilFileExist(hdfs, sku_path)) { sc.stop(); return }

        val sku = read_raw(sc, sku_path)
        spark.sql(
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
        }.repartition(20).saveAsTextFile(s"s3a://$bucket/$bucketTableName/$timestamp")

        mode.split(",").foreach {
            case "stage" => println(send_request(mode="stage", version=timestamp))
            case "prod" => println(send_request(mode="prod", version=timestamp))
        }
        println("end")
    }

//    def write(s: RDD[String], bucket: String, bucketTableName: String, version: String): Unit = {
//        s.repartition(100).mapPartitions { x =>
//            val idx = TaskContext.getPartitionId()
//            val filePath = "tmp" + File.separator + "index"
//            s3write.write2Disk(filePath, x)
//            val file = new File(filePath)
//            println(s"file length: ${file.length()}")
//            S3Handler.putObjectFile(filePath, bucket, s"$bucketTableName/$version/part-$idx")
//            file.delete()
//            x
//        }.count()
//    }


    def read_raw(sc: SparkContext, path: String): RDD[(String, Array[Float])] = {
        sc.textFile(path).map { row =>
            val id = row.split(",")(0)
            val emb = row.split(",").drop(1).map(_.toFloat)
            (id, emb)
        }
    }

    def send_request(mode: String, version:String): String = {
        val url = mode match {
            case "stage" => "http://10.176.17.101:8088/v1/tasks"
            case "prod" => "http://10.176.17.167:8088/v1/tasks"
        }

        val data = Json.parse(
            s"""
              |{
              |     "data_source": "com-sankuai-wmadrecall-hangu-admultirecall/ptU2ISkuEmb/$version",
              |     "data_source_type": "s3",
              |     "table_name": "PtVectorSg",
              |     "schema": "com-sankuai-wmadrecall-hangu-admultirecall/sku_vector_pt.proto",
              |     "output_type": "s3",
              |     "output_path": "com-sankuai-wmadrecall-hangu-admultirecall",
              |     "build_options": "forward.index.type=murmurhash,forward.index.hash.bucket.num=2097152, forward.segment.level=3,inverted.segment.level=6, inverted.term.index.type=murmurhash,inverted.term.index.hash.bucket.num=4194304",
              |     "owner": "yangyufeng04"
              |}
              |""".stripMargin).toString()
        val response = Http(url).postData(data).header("content-type", "application/json").asString.body
        response
    }
}
