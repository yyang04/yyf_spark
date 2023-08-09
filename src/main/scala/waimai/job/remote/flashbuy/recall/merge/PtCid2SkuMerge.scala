package waimai.job.remote.flashbuy.recall.merge

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import waimai.utils.{FileOp, JsonUtils}
import waimai.utils.SparkJobs.RemoteSparkJob
import waimai.utils.TimeOperations.getDateDelta
import waimai.utils.DateUtils.getNDaysAgo

object PtCid2SkuMerge extends RemoteSparkJob {
    // 将召回结果写入s3
    // 表名为 pt_cid2sku / pt_uuid2sku 目前线上就这两个表
    // pt_cid2sku:
    //    1. pt_cid_sales_sku_base
    //    2. pt_cid_sales_sku_discount

    override def run(): Unit = {
        val dt = params.dt match { case "" => getNDaysAgo(1); case x => x }
        val threshold = params.threshold   // 按照分数倒排取前k个
        val tableName = "pt_cid2sku"       // 一个表对应一个任务，如果不是的话否则没法依赖

        val result = spark.sql(
            s"""
               |select method_name,
               |       key,
               |       value
               |  from mart_waimaiad.pt_multi_recall_results_xxx2sku
               | where dt='$dt'
               |   and table_name='$tableName'
               |   and size(value) != 0
               |""".stripMargin).rdd.map{ row =>
            val methodNames = row.getString(0)
            val key = row.getString(1)
            val value = row.getAs[Map[Long, Float]](2)  // Map(Sku->Score)
            (key, (methodNames, value))
        }.groupByKey.map { case (key, iter) =>
            transform(key, iter.toArray, threshold)
        }.repartition(100)

        // 存储数据
        val dataPath = "/user/hadoop-hmart-waimaiad/ad/admultirecall/online_dict"
        val indexPath = s"/user/hadoop-hmart-waimaiad/ad/admultirecall/file_list"

        saveData(result, dataPath, dt, tableName)
        saveIndex(indexPath, dt, tableName)
    }

    private def saveData(result: RDD[String], dataPath: String, dt: String, tableName: String): Unit = {
        // 存入当天数据
        FileOp.saveAsTextFile(hdfs, result, path=s"$dataPath/$dt/$tableName")
        // 删除30天前的数据
        val dtOneMonthAgo = getDateDelta(dt, -30)
        FileOp.deleteTextFile(hdfs, path=s"$dataPath/$dtOneMonthAgo/$tableName")
    }
    private def saveIndex(indexPath: String, dt: String, tableName: String): Unit = {
        // index 日期在数据日期之后
        val indexDt = getDateDelta(dt, 1)
        val path = s"$indexPath/$indexDt"
        // 如果存在就Append进去，不存在就加上
        if (hdfs.exists(new Path(path))) {
            val originFiles = sc.textFile(path).collect().toBuffer.filterNot(_ contains tableName)
            originFiles.append(s"$dt/$tableName")
            FileOp.saveTextFile(hdfs, originFiles, path)
        } else {
            FileOp.saveTextFile(hdfs, Seq(s"$dt/$tableName").toBuffer, path)
        }
    }

    def transform(key: String, value: Array[(String, Map[Long, Float])], threshold: Int): String = {
        // "key" -> 字典构建的key
        // “value” -> Map(MethodName -> Map(sku -> Score))
        val methodSkuScoreJson = value.map{
            case (methodName, skuScore) =>
                val skuScoreJson = skuScore.toArray.sortBy(-_._2).take(threshold).map{
                    case (skuId, relatedScore) => JsonUtils.iterableToJsonObject(Map("skuId" -> skuId, "relatedScore" -> relatedScore))
                }
                JsonUtils.iterableToJsonObject(Map("methodName" -> methodName, "relatedSkus" -> JsonUtils.iterableToJsonArray(skuScoreJson)))
        }
        JsonUtils.iterableToJsonObjectStr(Map("cate3Id_geohash" -> key, "methods" -> JsonUtils.iterableToJsonArray(methodSkuScoreJson)))
    }
}
