package utils
import scala.reflect.ClassTag
import scala.collection.mutable
import com.alibaba.fastjson.{JSONArray,JSON}

object JSONUtils {

    def jsonObjectStrToMap[T: ClassTag](json: String): Map[String, T] = {
        val arr = new mutable.HashMap[String, T]()
        try {
            val jb = JSON.parseObject(json)
            val kSet = jb.entrySet().iterator()
            while (kSet.hasNext) {
                val kv = kSet.next()
                val k = kv.getKey
                val v = kv.getValue.asInstanceOf[T]
                arr.put(k, v)
            }
        } catch {
            case e: Exception => println("parse jsonobject error")
        }
        arr.toMap
    }

    def jsonArr2Arr[T: ClassTag](ja: JSONArray): Array[T] = {
        val n = ja.size()
        val rs = new Array[T](n)
        for (i <- 0 until n) {
            rs(i) = ja.get(i).asInstanceOf[T]
        }
        rs
    }

    def jsonPv(input: String): Map[String, Array[(Long, String)]] ={
        jsonObjectStrToMap[JSONArray](input)
          .mapValues { x =>
              jsonArr2Arr[JSONArray](x).map{ y =>
                  try { (y.getString(0).toLong, y.getString(1)) }
                  catch { case _: Exception => (0, "")}
              }
          }
    }

    //    def main(args: Array[String]): Unit = {
    //        val a = Map("lat" -> 51.235685, "long" -> -1.309197)
    //        val b = Json.toJson(a)
    //        val c = Json.toJson(
    //            Array(
    //                Json.obj(
    //                    "name" -> "Fiver",
    //                    "age" -> 4,
    //                    "role" -> JsNull
    //                ),
    //                Json.obj(
    //                    "name" -> "Bigwig",
    //                    "age" -> 6,
    //                    "role" -> "Owsla"
    //                )
    //
    //            ))
    //        println(c.toString())
    //
    //    }




}