//package utils
//
//import com.github.jelmerk.knn.scalalike.{Item, floatInnerProduct}
//import com.github.jelmerk.knn.scalalike.hnsw.HnswIndex
//
//case class TestItem(id: String, vector: Array[Float]) extends Item[String, Array[Float]] {
//    override def dimensions: Int = vector.length
//}
//
//object HNSWUtils {
//
//    def main(args: Array[String]): Unit = {
//
//        val item1 = TestItem("1", Array(0.0110f, 0.2341f))
//        val item2 = TestItem("2", Array(0.2300f, 0.3891f))
//        val item3 = TestItem("3", Array(0.4300f, 0.9891f))
//
//
//
//        val index = HnswIndex[String, Array[Float], TestItem, Float](dimensions=2, floatInnerProduct, maxItemCount=3, m=64, ef=200, efConstruction=200)
//
//        index.addAll(Seq(item1, item2, item3), listener = (workDone: Int, max: Int) =>
//            println(s"Added $workDone out of $max words to the index.")
//        )
//        val result = index.findNearest(item1.vector, k=2).map(x => (x.item().id, 1 - x.distance()))
//        result.foreach(println(_))
//
//
//    }
//
//
//
//}
