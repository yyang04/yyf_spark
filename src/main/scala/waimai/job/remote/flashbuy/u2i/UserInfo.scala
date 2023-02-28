package waimai.job.remote.flashbuy.u2i

import com.github.jelmerk.knn.scalalike.Item

case class UserInfo(id: String, vector: Array[Float]) extends Item[String, Array[Float]] {
    override def dimensions: Int = vector.length
}