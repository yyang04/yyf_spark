package job.remote.flashbuy.u2i
import utils.SparkJobs.RemoteSparkJob
import utils.FileOperations.saveAsTable

object SeqFeature extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.dt
        val src_table = params.src_table_name
        val dst_table = params.dst_table_name

        val init = spark.sql(
            s"""
              |select uuid, sku_list
              |  from $src_table
              | where dt=$dt
              |""".stripMargin).rdd.flatMap{ row =>
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
            sku_timestamp.map{
                case(sku_id, timestamp) => (sku_id, (uuid, timestamp))
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
               |   AND is_online_poi_flag = 1""".stripMargin).rdd.map{ row =>
            val sku_id = row.getLong(0)
            val tag_id = row.getString(1).toLong
            (sku_id, tag_id)
        }.join(init).map{ case (sku_id, (tag_id, (uuid, timestamp))) => (uuid, (tag_id, timestamp))
        }.groupByKey.mapValues{ iter =>
            iter.toList.sortBy(_._2).reverse.map(_._1)
        }.toDF("uuid", "tag_list")

        saveAsTable(spark, sku_info, dst_table, Map("dt" -> dt) )



    }

}
