package waimai.job.remote.flashbuy.s2i

import com.sankuai.waimai.aoi.core.index.AoiArea
case class Aoi (aoi_id: Long, aoi_type_id:Int, aoi_polygon: String) extends AoiArea {
    this.setId(aoi_id.toInt)
    override def toString = s"$aoi_id, $aoi_type_id, $aoi_polygon"
}