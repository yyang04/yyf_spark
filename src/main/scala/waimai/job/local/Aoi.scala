package waimai.job.local
import com.sankuai.waimai.aoi.core.index.AoiArea
case class Aoi (id: Int) extends AoiArea {
    this.setId(id)
    override def toString = s"$id"
}