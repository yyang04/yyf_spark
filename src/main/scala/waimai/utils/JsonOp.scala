package waimai.utils

import com.alibaba.fastjson.parser.ParserConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object JsonOp {

    ParserConfig.getGlobalInstance.setSafeMode(true)

    // {"name": "Tom"}  -> Map("name" -> "Tom")
    def jsonObjectStrToMap[T: ClassTag](json: String): Map[String, T] = {
        try {
            val jb = JSON.parseObject(json)
            jsonObjectToMap[T](jb)
        } catch {
            case _: Exception => println("parse jsonObject error")
                Map[String, T]()
        }
    }

    def jsonObjectToMap[T: ClassTag](jb: JSONObject): Map[String, T] = {
        val arr = new mutable.HashMap[String, T]()
        val kSet = jb.entrySet().iterator()
        while (kSet.hasNext) {
            val kv = kSet.next()
            val k = kv.getKey
            val v = kv.getValue.asInstanceOf[T]
            arr.put(k, v)
        }
        arr.toMap
    }


    // ["Apple", "Pear", "Grape"] -> Array("Apple", "Pear", "Grape")
    def jsonArrStrToArr[T: ClassTag](json: String): Array[T] = {
        val rs = new ArrayBuffer[T]
        try {
            val ja = JSON.parseArray(json)
            val n = ja.size()
            for (i <- 0 until n) {
                rs.append(ja.get(i).asInstanceOf[T])
            }
        } catch {
            case _: Exception => println("parse jsonArray error")
        }
        rs.toArray
    }

    // [{"name": "Tom", "gender": "female"}, {"name": "Bill", "gender": "male"}]
    // Array(Map("name" -> "Tom", "gender" -> "female"), Map("name" -> "Bill", "gender" -> "male"))
    def jsonObjectStrToArrayMap[T: ClassTag](json: String): Array[Map[String, T]] = {
        jsonArrStrToArr[JSONObject](json).map(jsonObjectToMap[T](_))
    }

    def iterableToJsonObjectStr(arr: Iterable[(String, Any)]): String = {
        iterableToJsonObject(arr).toJSONString
    }

    // Map("name" -> "Tom") -> {"name": "Tom"}
    def iterableToJsonObject(arr: Iterable[(String, Any)]): JSONObject = {
        val jb = new JSONObject()
        arr.foreach(x => jb.put(x._1, x._2))
        jb
    }

    def iterableToJsonArray(arr: Iterable[Any]): JSONArray = {
        val ja = new JSONArray()
        arr.foreach(h => ja.add(h))
        ja
    }
    def iterableToJsonArrayString(arr: Iterable[Any]): String = {
        iterableToJsonArray(arr).toJSONString
    }

    //    def jsonPv(input: String): Map[String, Array[(Long, String)]] ={
    //        jsonObjectStrToMap[JSONArray](input)
    //          .mapValues { x =>
    //              jsonArr2Arr[JSONArray](x).map{ y =>
    //                  try { (y.getString(0).toLong, y.getString(1)) }
    //                  catch { case _: Exception => (0L, "")}
    //              }
    //          }
    //    }


    def main(args: Array[String]): Unit = {
        val result: Array[Map[String, String]] = jsonObjectStrToArrayMap[String]("""[{"name": 1.0, "gender": "female"}, {"name": "Bill", "gender": "male"}]""")
    }

}