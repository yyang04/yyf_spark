package job.remote.flashbuy.u2i
import com.github.jelmerk.knn.scalalike.floatInnerProduct
import com.github.jelmerk.knn.scalalike.bruteforce.BruteForceIndex

object HnswTest {

    def main(args: Array[String]): Unit = {

        val dimensions = 2

        val item1 = SkuInfo("1", Array(0.0110f, 0.2341f))
        val item2 = SkuInfo("2", Array(0.2300f, 0.3891f))
        val item3 = SkuInfo("3", Array(0.4300f, 0.9891f))

        val index = BruteForceIndex[String, Array[Float], SkuInfo, Float](dimensions, floatInnerProduct)
        index.addAll(Seq(item1, item2, item3))
        val results = index.findNearest(item1.vector, 10)
        println(results)

        println(results)

    }






}
