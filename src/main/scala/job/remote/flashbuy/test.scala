package job.remote.flashbuy
import scala.math.exp
import org.apache.spark.rdd.RDD


object test {
    def main(args: Array[String]): Unit = {

    }
    def a(x:RDD[(String, (String, (String, String, String)))]):
         RDD[(String, Map[String, String], Map[String, String], Map[String, String])] ={
        val res = x.groupByKey.mapValues{ iter =>
            val a = iter.toMap
            (a.mapValues(_._1), a.mapValues(_._2), a.mapValues(_._3))
        }.map{
            case(k, (a,b,c)) => (k,a,b,c)
        }
        res
    }
    def b(r:List[Long]):Unit={
        r(1)
    }
}
