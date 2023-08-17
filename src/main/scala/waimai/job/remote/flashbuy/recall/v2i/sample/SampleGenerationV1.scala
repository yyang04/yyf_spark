package waimai.job.remote.flashbuy.recall.v2i.sample

import org.apache.spark.sql.functions.lit
import waimai.utils.DateOp.getNDaysAgoFrom
import waimai.utils.SparkJobs.RemoteSparkJob
import waimai.utils.{Sample, SampleOp}

case class PositiveSample(dt: String,
                          poi_id: Long,
                          sku_id: Long,
                          user_id: Long,
                          uuid: String,
                          city_id: Long,
                          event_id: String,
                          event_timestamp: Long,
                          hour: Int,
                          client_id: Int,
                          appversion: String
                         )

case class MixSample(dt: String,
                     poi_id: Long,
                     sku_id: Long,
                     user_id: Long,
                     uuid: String,
                     city_id: Long,
                     event_id: String,
                     event_timestamp: Long,
                     hour: Int,
                     client_id: Int,
                     appversion: String,
                     negative_sample: Array[Long]
                    )


object SampleGenerationV1 extends RemoteSparkJob {

    override def run(): Unit = {

        val endDt = params.endDt
        val beginDt = params.beginDt
        val window = params.window         // 正样本统计窗口
        val negSample = params.threshold   // 每个正样本的负样本个数

        // 从Hive表取出正样本做统计值
        val sample = spark.sql(
            s"""
               |select dt,
               |       poi_id,
               |       sku_id,
               |       spu_id,
               |       coalesce(user_id, 0) as user_id
               |       uuid,
               |       city_id,
               |       event_id,
               |       event_timestamp,
               |       hour,
               |       client_id,
               |       appversion
               |  from mart_waimaiad.pt_multirecall_sample
               |  where dt between ${getNDaysAgoFrom(endDt, window)} and $endDt
               |""".stripMargin).as[PositiveSample].rdd.cache

        // 计算负样本采样概率(从正样本里采样）
        val skuScore = sample.map{ x => (x.sku_id, 1) }
          .reduceByKey(_ + _)
          .map{
              case (sku_id, score) =>
              val modifyScore = scala.math.pow(score.toDouble, 0.75)  // 负样本采样概率
              Sample[Long](modifyScore, sku_id) }
          .collect

        val bcSkuScore = sc.broadcast(skuScore)

        sample.filter(dtRange.toArray contains _.dt).mapPartitions { iter =>
            val skuScoreValue = bcSkuScore.value
            val negSampleCol = SampleOp.weightedSampleWithReplacement(skuScoreValue, negSample * iter.length)
            iter.zipWithIndex.map {
                case (PositiveSample(dt, poi_id, sku_id, user_id, uuid, city_id, event_id, event_timestamp, hour, client_id, appversion), index) =>
                    MixSample(dt, poi_id, sku_id, user_id, uuid, city_id, event_id, event_timestamp, hour, client_id, appversion, negSampleCol.slice(index, index + negSample))
            }
        }.toDF.withColumn("version", lit("one_neg"))
          .write
          .mode("overwrite")
          .partitionBy("dt")
          .format("orc")
          .saveAsTable("mart_waimaiad.pt_multirecall_sample_with_negative")
    }
}
