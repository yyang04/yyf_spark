package job.remote.flashbuy
import play.api.libs.json._
import utils.FileOperations
import utils.SparkJobs.RemoteSparkJob

object I2IMerge extends RemoteSparkJob {
    override def run(): Unit = {
        val date = params.beginDt
        val methodsNames = params.algorithm.split(",")
        val result = spark.sql(
            s"""
               |select method, key, value
               |  from mart_waimaiad.recsys_linshou_multi_recall_results_v2
               | where dt = '$date'
               |   and branch='cid'
               |""".stripMargin).rdd.map{ row =>
            val method = row.getString(0)
            val key = row.getString(1)
            val value = row.getAs[Seq[String]](2).map{ x =>
                val arr = x.split(":")
                (arr(0).toLong, arr(1).toFloat)
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

        FileOperations.saveAsTextFile(hdfs, result, "/user/hadoop-hmart-waimaiad/ad/admultirecall/online_dict/20221024/pt_cid2sku")
    }

}
