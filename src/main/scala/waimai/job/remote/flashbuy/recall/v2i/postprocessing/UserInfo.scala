package waimai.job.remote.flashbuy.recall.v2i.postprocessing

import com.github.jelmerk.knn.scalalike.Item

case class UserInfo(id: String, vector: Array[Float]) extends Item[String, Array[Float]] {
    override def dimensions: Int = vector.length
}