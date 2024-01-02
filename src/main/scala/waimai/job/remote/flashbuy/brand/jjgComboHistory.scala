package waimai.job.remote.flashbuy.brand

import com.taobao.tair3.client.TairClient
import utils.TairUtil
import org.apache.commons.lang3.tuple.ImmutablePair
import waimai.utils.SparkJobs.RemoteSparkJob

import scala.collection.JavaConverters._
import scala.collection.mutable
import ch.hsr.geohash.GeoHash
import scala.concurrent.duration._

object jjgComboHistory extends RemoteSparkJob {

	override def run(): Unit = {

		val data = spark.sql(
			s"""
			   |select b.activity_id,
			   |       b.wm_poi_id,
			   |       b.spu_id,
			   |       c.latitude,
			   |       c.longitude
			   |  from upload_table.pt_sg_jjg_spu_poi a
			   |  join upload_table.pt_sg_jjg_info b on a.poi_id=b.wm_poi_id and a.spu_id=b.spu_id
			   |  join (
			   |     select wm_poi_id, latitude, longitude
			   |       from mart_waimai.aggr_poi_info_dd
			   |      where dt=20240101
			   |  ) c on c.wm_poi_id=b.wm_poi_id
			   |""".stripMargin).rdd.map{ row ⇒

			val activity_id = row.getLong(0)
			val poi_id = row.getLong(1)
			val spu_id = row.getLong(2)
			val latitude = row.getLong(3)
			val longitude = row.getLong(4)
			val geohash = GeoHash.withCharacterPrecision(latitude / 1000.0 / 1000.0, longitude / 1000.0 / 1000.0, 5)
			val key = s"Activity_Markup_Spu_Supply_${activity_id}_${geohash}"
			(key, new ImmutablePair(poi_id, spu_id))

		}.groupByKey.map{
			case (key, iter) ⇒
				(key, iter.toArray.toSet)
		}.collect


		data.foreach{ row ⇒
			println(row._1, row._2)
		}

		Thread.sleep(5.minutes.toMillis)

		val op = new TairClient.TairOption(500, 0.toShort, 86400000)
		val client = new TairUtil
		data.foreach{ case (key, value) ⇒
			addTair(client, 4, key, op, value)

		}
	}

	def getTair(client: TairUtil,
	            area: Short,
	            key: String,
	            op: TairClient.TairOption): Set[ImmutablePair[Long, Long]] = {

		val result = client.getListObject(key, area, op, classOf[ImmutablePair[Long, Long]])
		result match {
			case null ⇒ Set[ImmutablePair[Long, Long]]()
			case _ ⇒ result.asScala.toSet
		}
	}

	def addTair(client: TairUtil,
	            area: Short,
	            key: String,
	            op: TairClient.TairOption,
	            data: Set[ImmutablePair[Long, Long]]
	            ): Unit = {
		val result = getTair(client, area, key, op)
		val combineData = (result ++ data).toList.asJava
		client.putListObject(key, combineData, area, op)
	}
}
