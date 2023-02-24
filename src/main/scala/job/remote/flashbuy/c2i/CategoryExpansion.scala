package job.remote.flashbuy.c2i

import com.taobao.tair3.client.TairClient
import com.taobao.tair3.client.TairClient.TairOption
import utils.FileOperations
import utils.SparkJobs.RemoteSparkJob
import utils.TairUtil

object CategoryExpansion extends RemoteSparkJob {


    override def run(): Unit = {

        val result = spark.sql(
            s"""
               |select key, value
               | from mart_waimaiad.pt_sg_cate_map
               | where method='base'
               |  and dt='20230221'
               |  and value is not null
               |""".stripMargin).rdd.map { row =>
            (row.getLong(0).toString, row.getString(1))
        }.collect

        val client = new TairUtil
        val prefix = "pt_sg_cate1_"
        val tairOption = new TairClient.TairOption(500)

        val rs = new java.util.HashMap[String, String]()
        result.foreach { case (cate1, value) =>
            val key = prefix + cate1
            if (value != "") {
                rs.put(key, value)
            }
        }
        client.batchPutString(rs, 4, tairOption)
        spark.stop()
    }



//        val categoryMap = spark.sql(
//            s"""
//               |select first_category_id,
//               |       second_category_id
//               |  from mart_lingshou.dim_prod_product_sku_s_snapshot
//               | where dt = 20230221
//               |   and first_category_id is not null
//               |   and second_category_id is not null
//               |""".stripMargin).distinct.rdd.map{ row =>
//            val firstCategoryId = row.getLong(0)
//            val secondCategoryId = row.getLong(1)
//            (firstCategoryId, secondCategoryId)
//        }.collect
//        val cateMap = categoryMap.groupBy(_._1).mapValues(_.map(_._2))
//        val result = categoryMap.map{ case( cate1, cate2 ) =>
//            (cate2, cateMap(cate1).filter( _ != cate2).map{ x => s"$x:1.0"}.mkString(","))
//        }
//        val df = sc.makeRDD(result, 1).toDF("key", "value")
//        FileOperations.saveAsTable(spark, df, "pt_sg_cate_map", partition=Map("method" -> "base", "dt" -> "20230221"))


//
}
