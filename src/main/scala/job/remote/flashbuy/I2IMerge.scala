package job.remote.flashbuy
import org.apache.hadoop.fs.Path
import play.api.libs.json._
import utils.FileOperations
import utils.SparkJobs.RemoteSparkJob
import utils.TimeOperations.getDateDelta

object I2IMerge extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.beginDt
        val methodsNames = params.algorithm.split(",")
        val result = spark.sql(
            s"""
               |select method, key, value
               |  from mart_waimaiad.recsys_linshou_multi_recall_results_v2
               | where date = '$dt'
               |   and branch='cid'
               |""".stripMargin).rdd.map{ row =>
            val method = row.getString(0)
            val key = row.getString(1)
            val value = row.getAs[Seq[String]](2).map{ x =>
                val arr = x.split(":")
                (arr(0).toLong, arr(1).toFloat-0.00001)
            }
            (key, (method, value))
        }.groupByKey.map { case (key, iter) =>
            val kvIter = Json.toJson(
                iter.toArray.filter(x => methodsNames contains x._1).map{
                    case (methodName, relatedSkus) =>
                        Json.obj(
                            "relatedSkus" -> Json.toJson(relatedSkus.map {
                                case (skuId, relatedScore) =>
                                    Json.obj("skuId" -> skuId, "relatedScore" -> relatedScore)
                        }),
                        "methodName" -> methodName
                    )
              })
            Json.obj(
                "cate3Id_geohash" -> key,
                "methods" -> kvIter
            ).toString()
        }.repartition(100)



        FileOperations.saveAsTextFile(hdfs, result, s"/user/hadoop-hmart-waimaiad/ad/admultirecall/online_dict/$dt/pt_cid2sku")
        val path = s"/user/hadoop-hmart-waimaiad/ad/admultirecall/file_list/${getDateDelta(dt, 1)}"

        val p = new Path(path)
        if (hdfs.exists(p)) {
            val originFiles = sc.textFile(path).collect().toBuffer.filterNot(_ contains s"pt_cid2sku")
            originFiles.append(s"$dt/pt_cid2sku")
            FileOperations.saveTextFile(hdfs, originFiles, path)
        } else {
            FileOperations.saveTextFile(hdfs, Seq(s"$dt/pt_cid2sku"), path)
        }
        FileOperations.deleteTextFile(hdfs, s"/user/hadoop-hmart-waimaiad/ad/admultirecall/online_dict/${getDateDelta(dt, -30)}/pt_cid2sku")
    }

}
