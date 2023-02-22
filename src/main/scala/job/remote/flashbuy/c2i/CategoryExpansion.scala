package job.remote.flashbuy.c2i

import com.taobao.tair3.client.TairClient
import com.taobao.tair3.client.TairClient.TairOption
import utils.FileOperations
import utils.SparkJobs.RemoteSparkJob
import utils.TairUtil

object CategoryExpansion extends RemoteSparkJob {


    override def run(): Unit = {

        val categoryMap = spark.sql(
            s"""
               |select first_category_id,
               |       second_category_id
               |  from mart_lingshou.dim_prod_product_sku_s_snapshot
               | where dt = 20230221
               |   and first_category_id is not null
               |   and second_category_id is not null
               |""".stripMargin).distinct.rdd.map{ row =>
            val firstCategoryId = row.getLong(0)
            val secondCategoryId = row.getLong(1)
            (firstCategoryId, secondCategoryId)
        }.collect
        val cateMap = categoryMap.groupBy(_._1).mapValues(_.map(_._2))
        val result = categoryMap.map{ case( cate1, cate2 ) =>
            (cate2, cateMap(cate1).filter( _ != cate2).map{ x => s"$x:1.0"}.mkString(","))
        }
        val df = sc.makeRDD(result, 1).toDF("key", "value")
        FileOperations.saveAsTable(spark, df, "pt_sg_cate_map", partition=Map("method" -> "base", "dt" -> "20230221"))
        val client = new TairUtil
        val prefix = "pt_sg_cate1_"
        val tairOption = new TairClient.TairOption(500)
        result.foreach { case (cate1, value) =>
            val key = prefix + cate1.toString
            client.putString(key, value, 4, tairOption)
        }
    }
}
