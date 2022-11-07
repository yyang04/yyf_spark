package job
import com.alibaba.fastjson.{JSON, JSONArray}

import scala.collection.mutable
import scala.reflect.ClassTag
import ch.hsr.geohash.GeoHash

object test {
    def main(args: Array[String]): Unit = {

//        val a = """{15667082:[["11056257935","sku2sku"],["11056250637","salesku"],["11179110735","salesku"],["11249888170","salesku"],["11251547281","salesku"],["11251012773","salesku"],["11511115085","salesku"],["11213692468","salesku"],["11055084676","salesku"],["11265660688","salesku"],["11251259346","salesku"],["11056819833","salesku"],["11250962919","salesku"],["11056819824","salesku"],["11056079472","salesku"],["11054924918","salesku"],["11056356775","salesku"],["11215086019","salesku"],["11231827130","salesku"],["11180168639","salesku"],["11252051540","salesku"]],9919649:[["3491829029","salesku"],["3488241145","salesku"],["11303899861","salesku"],["3469711573","salesku"],["3583986305","salesku"],["4958398637","salesku"],["3486158840","salesku"],["3469907945","salesku"],["3468018685","salesku"],["9671922227","salesku"],["3468074701","salesku"],["9654022122","salesku"],["11436370211","salesku"],["3469911922","salesku"],["3726267062","salesku"],["3591456428","salesku"],["10769172021","salesku"],["7709790039","salesku"],["3491922377","salesku"],["3552576559","salesku"]],9571557:[["7710450845","salesku"],["7437823296","salesku"],["8227985781","salesku"],["10511187867","salesku"],["11107858491","salesku"],["8947161458","salesku"],["8947161459","salesku"],["8914115585","salesku"],["8914115586","salesku"],["4533189228","salesku"],["4460960137","salesku"],["8821665459","salesku"],["8989342273","salesku"],["8989342274","salesku"],["3180973375","salesku"],["4423375890","salesku"],["4423375891","salesku"],["4423375888","salesku"],["4423375889","salesku"],["7388055197","salesku"]],14051034:[["9863308846","sku2sku"],["10276238435","sku2sku"],["9772750496","sku2sku"],["10081457242","sku2sku"],["8880510787","salesku"],["8880510786","salesku"],["8880510785","salesku"],["11003567556","salesku"],["9031469418","salesku"],["8928378903","salesku"],["9876021732","salesku"],["9876021733","salesku"],["9286352338","salesku"],["8673968736","salesku"],["10512230645","salesku"],["8958499442","salesku"],["8875790747","salesku"],["9821969103","salesku"],["8673936552","salesku"],["8742808520","salesku"],["8673750296","salesku"],["8674270445","salesku"],["8674169580","salesku"],["8674169578","salesku"]],14989322:[["10059832460","salesku"],["11055837677","salesku"],["10387880255","salesku"],["10909899099","salesku"],["10105072746","salesku"],["10072378487","salesku"],["10949554836","salesku"],["10069291286","salesku"],["10480319978","salesku"],["10072372761","salesku"],["10069709931","salesku"],["10072569856","salesku"],["10387880256","salesku"],["11214411753","salesku"],["10491746518","salesku"],["10059335428","salesku"],["11375823292","salesku"],["10054329079","salesku"],["10060047168","salesku"],["10059503724","salesku"]]}"""
//        val b = jsonObjectStrToMap[JSONArray](a).map(x => (x._1, jsonArr2Arr[JSONArray](x._2).map(y => (y.getString(0).toLong, y.getString(1)))))
//        val c = b.values.flatten.toMap
//        c.foreach(k => println(k))
        val lat = 40020147*1.0/1000000
        val lon =  11646669*1.0/1000000
        val hash = GeoHash.withCharacterPrecision(lat, lon, 5)
        val base32 = hash.toBase32
        println("srO79")


    }

    def geoHash(latitude: Int, longitude: Int): String = {
        val lat = (latitude * 1.0) / 1000000
        val lon = (longitude * 1.0) / 1000000
        val hash = GeoHash.withCharacterPrecision(lat, lon, 5)
        val base32 = hash.toBase32
        base32
    }



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




}
