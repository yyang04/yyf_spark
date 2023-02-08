package job.remote.flashbuy.u2i.sample
import org.apache.spark.sql._
case class A(a: Option[Long], b: Option[Long])

object test {
    def main(args: Array[String]): Unit = {

        val row = Row(null, null)
        val a = String.valueOf(row.getAs[Long](0))
        println((a + "," + "123" + a).split(",").mkString("Array(", ", ", ")"))
        println((a + "," + "123" + a))
        val b = "123"
        println(b(0))
    }
}
