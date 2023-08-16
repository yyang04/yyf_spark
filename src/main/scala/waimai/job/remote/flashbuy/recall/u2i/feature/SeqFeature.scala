package waimai.job.remote.flashbuy.recall.u2i.feature

import waimai.utils.FileOp.saveAsTable
import waimai.utils.SparkJobs.RemoteSparkJob

object SeqFeature extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.dt
        action(src_table="mart_waimaiad.lingshou_user_click", dst_table="lingshou_user_click_tag_sanitized_v2", dt=dt)
        action(src_table="mart_waimaiad.lingshou_user_cart", dst_table="lingshou_user_cart_tag_sanitized_v2", dt=dt)
        action(src_table="mart_waimaiad.lingshou_user_order", dst_table="lingshou_user_order_tag_sanitized_v2", dt=dt)
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
               |       tag_id
               |  FROM mart_lingshou.dim_prod_product_sku_s_snapshot
               | WHERE dt='$dt'
               |   AND is_delete = 0
               |   AND is_valid = 1
               |   AND is_online_poi_flag = 1
               |   AND tag_id is not null AND tag_id != ''
               |   """.stripMargin).rdd.map { row =>
            val sku_id = row.getLong(0)
            val tag_id = row.getString(1).toLong
            (sku_id, tag_id)
        }.join(init).map { case (sku_id, (tag_id, (uuid, timestamp))) => (uuid, (tag_id, timestamp))
        }.groupByKey.mapValues { iter =>
            val tag_list = iter.toList.sortBy(_._2).reverse.map(_._1)
            val tag_map = tag_list.groupBy(identity).mapValues(_.size)
            val tag_seq_list = tag_list.distinct
            val tag_freq_list = tag_seq_list.map(x => tag_map(x))
            (tag_seq_list, tag_freq_list)
        }.map { case (uuid, (tag_list, tag_freq_list)) => (uuid, tag_list, tag_freq_list) }.toDF("uuid", "tag_list", "tag_freq_list")
        saveAsTable(sku_info, dst_table, Map("dt" -> dt))
    }
}
