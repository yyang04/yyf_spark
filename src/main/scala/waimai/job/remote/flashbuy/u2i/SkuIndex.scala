package waimai.job.remote.flashbuy.u2i

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import play.api.libs.json.{JsArray, JsNumber, Json}
import scalaj.http.Http

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Date
import waimai.utils.Json.JSONUtils
import waimai.utils.{FileOperations, S3Connect, S3Handler}
import waimai.utils.SparkJobs.RemoteSparkJob

object SkuIndex extends RemoteSparkJob with S3Connect {
    override def run(): Unit = {
        initConnect(sc)
        // 将sku embedding上传到s3，并加载到函谷
        val dt = params.dt                              // 需要导出sku向量日期
        val ts = params.timestamp.split(",")     // 模型时间戳，以逗号分割
        val version = params.version.split(",")  // 向量版本，以逗号分割，和模型时间戳长度一致
        val mode = params.mode.split(",")        // 存入向量 prod/stage，以逗号分割
        val config = params.config                      // 存入的proto文件名称，默认是这个 sku_vector_pt.proto

        val timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))  // s3 时间戳
        val utime = new Date().getTime                             // 存入table的时间戳
        val tableName = "PtVectorSg"                               // table名称
        val bucket = "com-sankuai-wmadrecall-hangu-admultirecall"  // s3地址
        val bucketTableName = "ptU2ISkuEmb"                        // s3地址

        require(ts.length==version.length)

        // 获取embedding
        val sku_emb = (ts, version).zipped
          .map{ case (x, y) => prepare_data(x, dt, y) }
          .reduce(_.union(_))
          .groupByKey.map{
            case (sku_id, iter) => (sku_id, iter.toMap)
        }

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
        }.join(sku_emb).map{
            case (sku_id, (poi_id, emb)) =>
                formatted_data(tableName, utime, sku_id, poi_id, emb)
        }.repartition(20).saveAsTextFile(s"s3a://$bucket/$bucketTableName/$timestamp")

        // 发送请求
        mode.foreach {
            case "stage" => println(send_request(mode="stage", version=timestamp, tableName=tableName, config=config))
            case "prod" => println(send_request(mode="prod", version=timestamp, tableName=tableName, config=config))
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


    def read_raw(sc: SparkContext, path: String, version: String): RDD[(String, (String, Array[Float]))] = {
        sc.textFile(path).map { row =>
            val id = row.split(",")(0)
            val emb = row.split(",").drop(1).map(_.toFloat)
            (id, (version, emb))
        }
    }

    def send_request(mode: String, version:String, tableName:String= "PtVectorSg", config: String="sku_vector_pt.proto"): String = {

        val url = mode match {
            case "prod" => "http://10.176.17.101:8088/v1/tasks"
            case "stage" => "http://10.176.17.167:8088/v1/tasks"
        }

        val data = Json.parse(
            s"""
              |{
              |     "data_source": "com-sankuai-wmadrecall-hangu-admultirecall/ptU2ISkuEmb/$version",
              |     "data_source_type": "s3",
              |     "table_name": "$tableName",
              |     "schema": "com-sankuai-wmadrecall-hangu-admultirecall/$config",
              |     "output_type": "s3",
              |     "output_path": "com-sankuai-wmadrecall-hangu-admultirecall",
              |     "build_options": "forward.index.type=murmurhash,forward.index.hash.bucket.num=2097152, forward.segment.level=3,inverted.segment.level=6, inverted.term.index.type=murmurhash,inverted.term.index.hash.bucket.num=4194304",
              |     "owner": "yangyufeng04"
              |}
              |""".stripMargin)
          .toString

        Http(url).postData(data).header("content-type", "application/json").asString.body
    }

    def prepare_data(ts: String, dt: String, version: String): RDD[(String, (String, Array[Float]))] = {
        val sku_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$ts/sku_embedding/$dt"
        if (!FileOperations.waitUntilFileExist(hdfs, sku_path)) {
            sc.stop()
        }
        read_raw(sc, sku_path, version)
    }

    def formatted_data(tableName: String, utime: Long, sku_id: String, poi: Long, emb: Map[String, Array[Float]]): String = {
        val data = Json.obj(
            "embV1" -> JsArray(emb.getOrElse("v1", Array[Float]()).map(JsNumber(_))),
            "embV2" -> JsArray(emb.getOrElse("v2", Array[Float]()).map(JsNumber(_))),
            "embV3" -> JsArray(emb.getOrElse("v3", Array[Float]()).map(JsNumber(_))),
            "poiId" -> poi,
            "skuId" -> sku_id.toLong,
        ).toString
        // return
        Json.obj("table" -> tableName, "utime" -> utime, "data" -> data, "opType" -> 1, "pKey" -> sku_id).toString
    }

}
