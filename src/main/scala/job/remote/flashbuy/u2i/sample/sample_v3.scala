package job.remote.flashbuy.u2i.sample

import utils.{FileOperations, SampleOperations, Sample}
import utils.SparkJobs.RemoteSparkJob
import utils.TimeOperations.getDateDelta

import scala.util.Random


object sample_v3 extends RemoteSparkJob{

    override def run(): Unit = {
        // 正样本扩充并按照采样方式采样
        // 负样本也采样
        // 不要用搜索数据，就用自然数据，一定要归一化，不归一化全都完了
        val dt = params.dt
        val threshold = params.threshold
        val dst_table_name = params.dst_table_name

        // 候选 poi 池
        val sku_pool = spark.sql(
            s"""
               |SELECT distinct poi_id
               |  FROM mart_waimaiad.pt_flashbuy_expose_poi_daily_v1
               | WHERE dt BETWEEN '${getDateDelta(dt, -10)}' AND '$dt'
               |""".stripMargin).rdd.map { row =>
            val poi_id = row.getLong(0)
            (poi_id, 0L)
        }

        // 正样本
        val sku_pos_tmp = spark.sql(
            s"""
               |SELECT event_type,
               |       request_id,
               |       uuid,
               |       user_id,
               |       sku_id,
               |       cast(spu_id as bigint) as spu_id,
               |       poi_id
               |  FROM mart_lingshou.fact_flow_sdk_product_mv
               | WHERE dt='$dt'
               |   AND uuid is not null
               |   AND uuid != ''
               |   AND sku_id is not null
               |   AND poi_id is not null
               |   AND event_id='b_xU9Ua'
               |   AND category_type=13
               |""".stripMargin
        ).as[ModelSample].rdd.distinct.map { sample => (sample.poi_id, sample) }.join(sku_pool).map{ _._2._1 }.cache
        val total_count = sku_pos_tmp.count().toDouble
        val sku_pos_count = sku_pos_tmp.map{ x => ((x.sku_id, x.spu_id), 1d) }.reduceByKey(_+_)
        val sku_pos = sku_pos_tmp.map(x => ((x.sku_id, x.spu_id), x)).join(sku_pos_count).map{ x => Sample(norm_pos(x._2._2), x._2._1) }
        val sample_sku_pos = SampleOperations.sampleWeightedRDD[ModelSample](sku_pos, total_count.toInt).map(x => (x.poi_id, x))

        val sku_neg = spark.sql(
            s"""
               |SELECT product_id, product_spu_id, poi_id
               |  FROM mart_lingshou.dim_prod_product_sku_s_snapshot mv
               | WHERE dt = '$dt'
               |   AND sell_status = '0'
               |   AND product_status = '0'
               |   AND is_valid = 1
               |   AND is_online_poi_flag = 1
               |""".stripMargin).rdd.map { row =>
            val sku_id = row.getAs[Long](0)
            val spu_id = row.getAs[Long](1)
            val poi_id = row.getLong(2)
            val sample = ModelSample(sku_id=sku_id, spu_id=Some(spu_id), poi_id=poi_id)
            (sample.poi_id, sample)
        }.join(sku_pool)
          .map(_._2._1)
          .map(x => ((x.sku_id, x.spu_id), x))
          .leftOuterJoin(sku_pos_count)
          .values
          .map{
            case (x, score) => (x.poi_id, Sample(1d, x))
        }.groupByKey.join(sample_sku_pos).values.flatMap {
            case (iter, x_pos) =>
                SampleOperations.weightedSampleWithReplacement(iter.toArray, threshold, new Random)
                .map{ x => ModelSample("view", x_pos.request_id, x_pos.uuid, x_pos.user_id, x.sku_id, x.spu_id, x.poi_id) } :+ x_pos
        }.map{
            case ModelSample(event_type, request_id, uuid, user_id, sku_id, spu_id, poi_id) =>
                (event_type, request_id, uuid, user_id, sku_id, spu_id, poi_id)
        }.toDF("event_type", "request_id", "uuid", "user_id", "sku_id", "spu_id", "poi_id")

        FileOperations.saveAsTable(spark, sku_neg, dst_table_name, Map("dt" -> s"$dt", "threshold" -> s"$threshold"))
    }

    def norm_pos(freq: Double): Double = {
        (math.sqrt(freq / 0.001) + 1) * 0.001 / freq
    }

    def norm_neg(freq: Double): Double = {
        math.pow(freq, 0.25)
    }
}
