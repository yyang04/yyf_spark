package waimai.utils

import com.alibaba.fastjson.parser.ParserConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object JsonUtils {

    ParserConfig.getGlobalInstance.setSafeMode(true)

    def jsonObjectStrToMap[T: ClassTag](json: String): Map[String, T] = {
        // {String: T}
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
            case _: Exception => println("parse jsonObject error")
        }
        arr.toMap
    }

    def jsonObjectStrToArrayMap[T: ClassTag](json: String): Array[Map[String, T]] = {
        // [{String: T} , {String, T}]
        val arr = new ArrayBuffer[Map[String, T]]
        try {
            val ja = JSON.parseArray(json)
            var idx = 0
            while (idx < ja.size()) {
                val jb = ja.get(idx).toString
                val m = jsonObjectStrToMap[T](jb)
                arr.append(m)
                idx += 1
            }
        } catch {
            case _: Exception => println("parse jsonArray error")
        }
        arr.toArray
    }

    private def jsonArr2Arr[T: ClassTag](ja: JSONArray): Array[T] = {
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


    private def jsonObjectStrToArray[T: ClassTag](json: String): Array[(String, T)] = {
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
            case _: Exception => println("parse jsonObject error")
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

    def main(args: Array[String]): Unit = {
        val a = jsonObjectStrToArrayMap("[{\"bid\":65.0,\"bid_ocpm\":0.9622094426304101,\"bid_ocpm_ratio\":1.0,\"charge_mode\":1,\"charge_rate\":0.0,\"gmv\":0.13727099924973363,\"gmv_k1\":0.12,\"gmv_no_k\":1.1439249937477802,\"gsp_price\":0.0,\"pctr\":0.014803222194314002,\"pcvr\":0.7727540522813797,\"poi\":17046340,\"price\":0.0,\"rank_score\":1.0994804418801438,\"unit\":37079002},{\"bid\":110.0,\"bid_ocpm\":2.870927631855011,\"bid_ocpm_ratio\":1.0,\"charge_mode\":1,\"charge_rate\":0.0,\"gmv\":1.6175536642090065,\"gmv_k1\":0.12,\"gmv_no_k\":13.479613868408387,\"gsp_price\":0.0,\"pctr\":0.026099342107772826,\"pcvr\":5.164733200073242,\"poi\":1093919,\"price\":0.0,\"rank_score\":4.488481296064018,\"unit\":26783094},{\"bid\":80.0,\"bid_ocpm\":3.3212842941284175,\"bid_ocpm_ratio\":1.0,\"charge_mode\":1,\"charge_rate\":0.0,\"gmv\":6.818439337837525,\"gmv_k1\":0.12,\"gmv_no_k\":56.82032781531271,\"gsp_price\":0.0,\"pctr\":0.04151605367660522,\"pcvr\":13.686350889205933,\"poi\":15723035,\"price\":0.0,\"rank_score\":10.139723631965943,\"unit\":27701717},{\"bid\":80.0,\"bid_ocpm\":2.0324150919914246,\"bid_ocpm_ratio\":1.0,\"charge_mode\":1,\"charge_rate\":0.0,\"gmv\":0.6312036036034141,\"gmv_k1\":0.12,\"gmv_no_k\":5.260030030028451,\"gsp_price\":0.0,\"pctr\":0.025405188649892805,\"pcvr\":2.0704550170898437,\"poi\":14614733,\"price\":0.0,\"rank_score\":2.6636186955948387,\"unit\":26040989}]")
        println(1)
    }
}