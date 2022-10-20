






package job.remote.flashbuy

package com.meituan.waimai.ad.lingshou.high_quality_sku

import com.meituan.waimai.ad.utils.{DateUtils, SparkOperation}

import scala.collection.mutable.ListBuffer

case class SkuEntity(sku_id: String, sku_sales_count: Long, poi_id: Long, cid3: Long,
                     tag_id: Long, cid3_geohash_code: String, geohash_code: String)

class LingShouHighQualitySku {
    def genHighQualitySku(today: String): Unit = {
        val lingshouHighQualitySkuByOrderTable = "mart_waimaiad.lingshou_high_quality_sku_by_order"
        val brandGeohash2Sku = ""
        val cidGeohash2Sku = "mart_waimaiad.lingshou_cid_geohashcode_2_sku"
        val tagGeohash2Sku = ""
        val lingshouHighQualityQueryTable = "mart_waimaiad.lingshou_high_quality_query"
        // 过去360天销量>=3
        highQualitySkuByOrder(today, days = 360, "mart_waimaiad.lingshou_user_order_pv_fixed", lingshouHighQualitySkuByOrderTable)
        Cid2Sku(today, days = 360, inputTable = lingshouHighQualitySkuByOrderTable, outputTable = cidGeohash2Sku, maxLen = 100)
    }


    def creathehighQualitySkuByOrderTable(tableName: String): String = {
        val SQL = s"create table if not exists ${tableName} (\n" +
          " sku_id string, " +
          s" sku_sales_count long," +
          s" poi_id long," +
          s" cid1 long," +
          s" cid2 long," +
          s" cid3 long," +
          s" brand_id long," +
          s" tag_id long," +
          s" latitude double," +
          s" longitude double," +
          s" geohash_code string" +
          ")" +
          "partitioned by (dt string)  STORED AS ORC"
        SQL
    }

    def createUserQueryBehavior(tableName: String): String = {
        val SQL = s"create table if not exists ${tableName} (\n" +
          " uuid string, " +
          s" timestamp_query string" +
          ")" +
          "partitioned by (dt string)  STORED AS ORC"
        SQL
    }

    // 过去N天销量产出 cid2sku,brand2sku,tag2sku,poi2sku 词表
    def highQualitySkuByOrder(today: String, days: Int, inputTable: String, outputTable: String): Unit = {
        val spark = SparkOperation.spark
        import spark.implicits._
        val createTableSQL = creathehighQualitySkuByOrderTable(outputTable)
        SparkOperation.sqlContext.sql(createTableSQL)
        println("createTableSQL: " + createTableSQL)

        val endDate = DateUtils.getDateByDelta(today, 0)
        val beginDate = DateUtils.getDateByDelta(today, -days)
        SparkOperation.sqlContext.sql("CREATE TEMPORARY FUNCTION mt_geohash AS 'com.sankuai.meituan.hive.udf.UDFGeoHash'")
        val highQualitySkuByOrderSQL =
            s"""
               |select sku_id,
               |       sku_sales_count,
               |       c.poi_id,
               |       cid1,
               |       cid2,
               |       cid3,
               |       brand_id,
               |       tag_id,
               |       latitude,
               |       longitude,
               |       mt_geohash(latitude /1000000.0, longitude /1000000.0, 5) as geohash_code
               |        from (
               |        select a.sku_id as sku_id,
               |               a.sku_sales_count as sku_sales_count,
               |               poi_id,
               |               cid1,
               |               cid2,
               |               cid3,
               |               brand_id,
               |               tag_id
               |          from (
               |                select sku_id,
               |                       count(sku_id) as sku_sales_count
               |                  from mart_waimaiad.lingshou_user_order_pv_fixed
               |                 where dt>='${beginDate}'
               |                   and dt<='${endDate}'
               |                 group by sku_id
               |                having count(*) >= 3
               |               )a
               |         inner join(
               |                select product_id as sku_id,
               |                       poi_id as poi_id,
               |                       first_category_id as cid1,
               |                       second_category_id as cid2,
               |                       third_category_id as cid3,
               |                       standard_brand_id as brand_id,
               |                       tag_id as tag_id
               |                  from mart_lingshou.dim_prod_product_sku_s_snapshot
               |                 where dt = '${endDate}'
               |               )b
               |            on a.sku_id=b.sku_id
               |       ) c
               | inner join(
               |        select poi_id,
               |               cast(latitude as double) as latitude,
               |               cast(longitude as double) as longitude
               |          from mart_lingshou.aggr_poi_info_dd
               |         where dt='${endDate}'
               |       ) d
               |    on c.poi_id=d.poi_id
               |
               |
       """.stripMargin

        val insertSQL =
            s"""
               | insert overwrite table ${outputTable} partition(dt='${today}')
               | ${highQualitySkuByOrderSQL}
       """.stripMargin
        SparkOperation.sqlContext.sql(insertSQL)
    }

    def Cid2Sku(today: String, days: Int, inputTable: String, outputTable: String, maxLen: Int): Unit = {
        val spark = SparkOperation.spark
        import spark.implicits._
        val highQualitySkuByOrderSQL =
            s"""
               |select sku_id,
               |       sku_sales_count,
               |       poi_id,
               |       cid3,
               |       tag_id,
               |       concat_ws('_', cast (cid3 as string),geohash_code) as cid3_geohash_code,
               |       geohash_code
               | from ${inputTable}
               | where dt='${today}'
       """.stripMargin
        println(s"highQualitySkuByOrderSQL sql:\n${highQualitySkuByOrderSQL}")
        val skuInfo = SparkOperation.sqlContext.sql(highQualitySkuByOrderSQL)
        //val groupSkuInfo = skuInfo.rdd.groupBy(x=>x.getAs[String]("cid3_geohash_code")).saveAsTextFile("viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/huangkun13/lingshou/session_data/test")

        val groupSkuInfo = skuInfo.rdd.groupBy(x=>x.getAs[String]("cid3_geohash_code")).map({
            case (cid3_geohash_code, entityList) => {
                val sortList = entityList.toList.sortBy(x=>x.getAs[Long](1)).takeRight(maxLen)
                //val sortList = entityList.toList
                val topSkuList: ListBuffer[String] = ListBuffer()
                val skuCountList: ListBuffer[String] = ListBuffer()
                for (skuEntity <- sortList) {
                    val sku_id:String = skuEntity.getAs[String](0)
                    //val cid3_geohash_code=skuEntity.cid3_geohash_code
                    val sku_sales_count:Long = skuEntity.getAs[Long](1)
                    //topSkuList.append((sku_id, sku_sales_count.toString))
                    topSkuList.append(sku_id)
                    skuCountList.append(sku_sales_count.toString)
                }
                (cid3_geohash_code, topSkuList.mkString(" "), skuCountList.mkString(" "))
                //(cid3_geohash_code, sortList.mkString("_"))
            }
        }).repartition(10).saveAsTextFile("viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/huangkun13/lingshou/session_data/cid3_high_sku/"+today)
        //.toDF("cid3_geohash_code", "sku_id_n_count")

        /*
        val temp_input_data = "temp_input_data"
        groupSkuInfo.createOrReplaceTempView(temp_input_data)

        val insertSQL =
          s"""
             | insert overwrite table ${outputTable} partition(dt='${today}')
             | select cid3_geohash_code,sku_id_n_count from ${temp_input_data}
             """.stripMargin
        println(s"insert sql:\n${insertSQL}")
        SparkOperation.sqlContext.sql(insertSQL)
         */

    }

}