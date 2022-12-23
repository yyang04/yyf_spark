package utils
import scala.reflect.ClassTag
import scala.collection.mutable
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.alibaba.fastjson.parser.ParserConfig

import scala.collection.mutable.ArrayBuffer

object JSONUtils {
    ParserConfig.getGlobalInstance.setSafeMode(true)

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
                  catch { case _: Exception => (0L, "")}
              }
          }
    }


    def jsonObjectStrToArray[T: ClassTag](json: String): Array[(String, T)] = {
        val arr = new ArrayBuffer[(String, T)]()
        try {
            val jb = JSON.parseObject(json)
            val kSet = jb.entrySet().iterator()
            while (kSet.hasNext) {
                val kv = kSet.next()
                val k = kv.getKey
                val v = kv.getValue.asInstanceOf[T]
                arr.append((k, v))
            }
        } catch {
            case e: Exception => println("parse jsonobject error")
        }
        arr.toArray
    }

    def jsonArrStr2Array[T: ClassTag](json: String): Array[T] = {
        val result = new ArrayBuffer[T]()
        try {
            val ja = JSON.parseArray(json)
            var idx = 0
            while (idx < ja.size()) {
                val jb = ja.get(idx).toString
                result.append(jb.asInstanceOf[T])
                idx += 1
            }
        } catch {
            case e: Exception => println("parse jsonArray error")
        }
        result.toArray
    }


    def iterableToJsonObjectStr(arr: Iterable[(String, Any)]): String = {
        iterableToJsonObject(arr).toJSONString
    }

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


    def nestedArray2JsonObjectStr(arr: Array[(String, Array[(String, Double)])]): String = {
        val jb = new JSONObject()
        arr.map { case (idx, midArr) =>
            val midJb = new JSONObject()
            midArr.foreach(x => midJb.put(x._1, x._2))
            (idx, midJb)
        }.foreach(x => jb.put(x._1, x._2))
        jb.toJSONString
    }

    def nestedJsonObjectStr2Array(json: String): Array[(String, Array[(String, Double)])] = {
        jsonObjectStrToArray[String](json).map(x => (x._1, jsonObjectStrToArray[Double](x._2)))
    }


    def params2Array(json: String): Array[(String, String)] = {
        val result = new ArrayBuffer[(String, String)]()
        try {
            val ja = JSON.parseArray(json)
            var idx = 0
            while (idx < ja.size()) {
                val jb = ja.get(idx).toString
                val arr = jsonObjectStrToMap[String](jb)
                if (arr.contains("I") && arr.contains("V")) {
                    val k = arr("I")
                    val v = arr("V")
                    result.append((k, v))
                }
                idx += 1
            }
        } catch {
            case e: Exception => println("parse jsonArray error")
        }
        result.toArray
    }
}