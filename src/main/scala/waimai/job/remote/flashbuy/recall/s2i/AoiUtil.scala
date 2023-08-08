package waimai.job.remote.flashbuy.recall.s2i

import com.sankuai.waimai.aoi.core.index.WmAoiIndexBuilder
import org.apache.spark.sql.SparkSession
import waimai.utils.DateUtils

object AoiUtil {
    def getAoiInstance(implicit spark: SparkSession): WmAoiIndexBuilder#WmAoiIndex = {
        val dt = DateUtils.getNDaysAgo(1)
        val builder = new WmAoiIndexBuilder().buffer(0.0002)
        spark.sql(
            s"""
               |select aoi_id, aoi_type_id, aoi_polygon, aoi_name
               |  from mart_waimai.topic_feature_aoi_d_view
               |  where dt = $dt and aoi_id is not null and aoi_type_id is not null
               |""".stripMargin).rdd.map{ row =>
            val aoi_id = row.getAs[Long](0)
            val aoi_type_id = row.getAs[Int](1)
            val aoi_polygon = row.getAs[String](2)
            val aoi_name = row.getAs[String](3)
            Aoi(aoi_id, aoi_type_id, aoi_polygon, aoi_name)
        }.collect.foreach { x =>
            builder.index[Aoi](x.aoi_polygon, x)
        }
        builder.build
    }
}
