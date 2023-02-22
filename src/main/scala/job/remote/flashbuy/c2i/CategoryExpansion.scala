package job.remote.flashbuy.c2i

import utils.FileOperations
import utils.SparkJobs.RemoteSparkJob

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
            (cate2, cateMap(cate1).map{ x => s"$x:1.0"}.mkString(","))
        }
        val df = sc.makeRDD(result, 1).toDF("key", "value")
        FileOperations.saveAsTable(spark, df, "pt_sg_cate_map", partition=Map("method" -> "base", "dt" -> "20230221"))

    }
}
