package job.remote.flashbuy.u2i.Feature

import utils.FileOperations.saveAsTable
import utils.SparkJobs.RemoteSparkJob

object InstantSeqFeature extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.dt
        action(src_table="mart_waimaiad.lingshou_user_cart", dst_table="lingshou_user_cart_tag_instant", dt=dt)
        action(src_table="mart_waimaiad.lingshou_user_order", dst_table="lingshou_user_order_tag_instant", dt=dt)
    }

    def action(src_table:String, dst_table:String, dt:String):Unit = {
        val init = spark.sql(
            s"""
               |select uuid, sku_list
               |  from $src_table
               | where dt=$dt
               |""".stripMargin).rdd.flatMap { row =>
            val uuid = row.getString(0)
            val sku_list = row.getSeq[String](1)
            val sku_timestamp = sku_list.flatMap { sku_timestamp =>
                try {
                    val arr = sku_timestamp.split("_").map(_.toLong)
                    List((arr(0), arr(1)))
                } catch {
                    case _: Throwable => None
                }
            }
            sku_timestamp.map {
                case (sku_id, timestamp) => (sku_id, (uuid, timestamp))
            }
        }
        val sku_info = spark.sql(
            s"""
               |SELECT product_id AS sku_id,
               |       coalesce(first_category_id, 0),
               |       coalesce(second_category_id, 0),
               |       coalesce(third_category_id, 0),
               |       cast(coalesce(tag_id, '0') as bigint) as tag_id
               |  FROM mart_lingshou.dim_prod_product_sku_s_snapshot
               | WHERE dt='$dt'
               |   AND is_delete = 0
               |   AND is_valid = 1
               |   AND is_online_poi_flag = 1
               |   """.stripMargin).as[SeqEntity].rdd.map{ x => (x.sku_id, x)}
          .join(init)
          .map { case (sku_id, (entity, (uuid, timestamp))) => (uuid, (entity, timestamp)) }
          .groupByKey
          .map { case( uuid, iter) =>
            val result = iter.toList.sortBy(-_._2).take(100)
            val timestamp_list = result.map(_._2)
            val first_category_id_list = result.map(_._1.first_category_id)
            val second_category_id_list = result.map(_._1.second_category_id)
            val third_category_id_list = result.map(_._1.third_category_id)
            val tag_id_list = result.map(_._1.tag_id)
              (uuid, timestamp_list, first_category_id_list, second_category_id_list, third_category_id_list, tag_id_list)}
          .toDF("uuid", "timestamp", "first_category_id", "second_category_id", "third_category_id", "tag_id")
        saveAsTable(spark, sku_info, dst_table, Map("dt" -> dt))
    }
}
