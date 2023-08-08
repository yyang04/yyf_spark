package waimai.job.remote.flashbuy.recall.s2i

import com.sankuai.waimai.aoi.core.index.AoiArea
case class Aoi (aoi_id: Long, aoi_type_id:Int, aoi_polygon: String, aoi_type_name: String) extends AoiArea {
    this.setId(aoi_id.toInt)
    override def toString = s"$aoi_id, $aoi_type_id, $aoi_type_name, $aoi_polygon"
}