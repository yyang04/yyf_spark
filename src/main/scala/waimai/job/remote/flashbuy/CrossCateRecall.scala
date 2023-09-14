package waimai.job.remote.flashbuy

import com.alibaba.fastjson.JSON
import com.taobao.tair3.client.TairClient
import sankuai.CrossCategorySkuInfo
import utils.TairUtil
import waimai.utils.FileOp
import waimai.utils.SparkJobs.RemoteSparkJob

import scala.concurrent.duration._
import scala.collection.JavaConverters._

case class SkuInfo (poi_id: Long,
                    poi_name: String,
                    sku_id: Long,
                    sku_name: String,
                    spu_id: Long,
                    first_category_id: Long,
                    second_category_id: Long,
                    third_category_id: Long,
                    var is_xp: Int
                   )

case class PoiSkuInfo (poiId: Long, skus: Array[SkuInfo])

object CrossCateRecall extends RemoteSparkJob {

	val prefix = s"ptsj_cross_cate_"

	override def run(): Unit = {
		val mode = params.mode
		val version = params.version
		if (mode == "test") {
			testTair()
			return
		}


		val xp = spark.sql(
			s"""
			   |select poi_id,
			   |       poi_name,
			   |       sku_id,
			   |       sku_name,
			   |       spu_id,
			   |       coalesce(info.first_category_id, 0) as first_category_id,
			   |       coalesce(info.second_category_id, 0) as second_category_id,
			   |       coalesce(info.third_category_id, 0) as third_category_id,
			   |       0 as is_xp
			   |  from upload_table.apple_0915_sku_info t
			   |  left join
			   |  (
			   |    select product_id,first_category_id,second_category_id,third_category_id
			   |      from mart_lingshou.dim_prod_product_sku_s_snapshot
			   |   where dt=20230912
			   |  ) info on t.sku_id=info.product_id
			   |  where sku_id is not null
			   |    and spu_id is not null
			   |    and poi_id is not null
			   |""".stripMargin).as[SkuInfo]

		val total = spark.sql(
			s"""
			   |select poi_id,
			   |       poi_name,
			   |       product_id as sku_id,
			   |       product_name as sku_name,
			   |       product_spu_id as spu_id,
			   |       coalesce(first_category_id, 0) as first_category_id,
			   |       coalesce(second_category_id, 0) as second_category_id,
			   |       coalesce(third_category_id, 0) as third_category_id,
			   |       1 as is_xp
			   |  from mart_lingshou.dim_prod_product_sku_s_snapshot sku_info
			   |  JOIN (
			   |     select poi_id as wm_poi_id
			   |       from upload_table.apple_0915_sku_info
			   |      group by 1
			   |  ) poi_info on sku_info.poi_id=poi_info.wm_poi_id
			   |  where dt = 20230912
			   |    AND is_delete = 0
			   |    AND is_valid = 1
			   |    AND is_online_poi_flag = 1
			   |    and product_id is not null
			   |    and product_spu_id is not null
			   |    and poi_id is not null
			   |""".stripMargin).as[SkuInfo]

		val result = xp.union(total).rdd.map{ skuInfo ⇒ (skuInfo.poi_id, skuInfo) }
		  .groupByKey
		  .map{ case (poi_id, iter) ⇒
			  val skuList = iter.groupBy(_.spu_id).values.map { skuInfoList ⇒
				  val is_xp = skuInfoList.map(_.is_xp).min
				  val result = skuInfoList.head
				  result.is_xp = is_xp
				  result
			  }
			  val left = skuList.filter(_.is_xp == 0).take(10)
			  val right = skuList.filter(_.is_xp == 1).take(20)
			  PoiSkuInfo(poi_id, (left ++ right).take(20).toArray)
		  }.cache

		if (mode == "test") {
			testTair()
			return
		}

		if (mode != "stage") {
			saveTair(result.collect, 15.days.toSeconds.toInt)
		}

		val df = result.map { poiSkuInfo ⇒
			val skus = poiSkuInfo.skus
			(skus.head.poi_id, skus.head.poi_name, skus.map(_.sku_id), skus.map(_.spu_id), skus.map(_.is_xp), skus.map(_.sku_name), skus.map(_.first_category_id))
		}.toDF("poiId", "poi_name", "skuId", "spuId", "is_xp", "sku_name", "first_category_id")

		FileOp.saveAsTable(df, "pt_sg_apple_new_sku", Map("version" -> version))

	}


	def saveTair(poiSkuInfos: Array[PoiSkuInfo], expire: Int): Unit = {
		val op = new TairClient.TairOption(300, 0.toShort, expire)
		val tair = new TairUtil

		poiSkuInfos.foreach { poiSkuInfo ⇒
			    val key = prefix + poiSkuInfo.poiId.toString
				val value = poiSkuInfo.skus.map { skuInfo ⇒
					val skuObj = new CrossCategorySkuInfo
					skuObj.setId(skuInfo.sku_id)
					skuObj.setSpuId(skuInfo.spu_id)
					skuObj.setFirstCategoryId(skuInfo.first_category_id)
					skuObj.setSecondCategoryId(skuInfo.second_category_id)
					skuObj.setThirdCategoryId(skuInfo.third_category_id)
					skuObj
				}.toList.asJava
			tair.putListObject(key, value, 4, op)
			Thread.sleep(500)
		}
	}

	def testTair(): Unit = {
		val testData = Array(PoiSkuInfo(13404273, Array(
			SkuInfo(13404273, "123", 7918052776L, "345", 6470074871L, 0, 0, 0, 0),
			SkuInfo(13404273, "123", 7485735025L, "345", 6144884144L, 0, 0, 0, 0),
			SkuInfo(13404273, "123", 7880070936L, "345", 6443917759L, 0, 0, 0, 0),
			SkuInfo(13404273, "123", 7918754899L, "345", 6470886832L, 0, 0, 0, 0),
			SkuInfo(13404273, "123", 11608370725L, "345", 8867731320L, 0, 0, 0, 0),
			SkuInfo(13404273, "123", 11608370724L, "345", 8867731320L, 0, 0, 0, 0),
			SkuInfo(13404273, "123", 7485431908L, "345", 6143585652L, 0, 0, 0, 0)
		)))
		saveTair(testData, 12.hour.toSeconds.toInt)
	}

}