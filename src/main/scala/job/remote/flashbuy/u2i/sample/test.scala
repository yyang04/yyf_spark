package job.remote.flashbuy.u2i.sample
import org.apache.spark.sql._


object test {
    def main(args: Array[String]): Unit = {


        val row = Row(null, null)
        val a = row.getAs[String](0)
        println(a(0))
        val b = "123"
        println(b(0))
    }



}
