package waimai.job.local

import ch.hsr.geohash.GeoHash
import scalaj.http.{Http, HttpResponse}
import waimai.utils.HashOp.hashString

object test {
    def main(args: Array[String]): Unit = {

        println(hashString("poi_id_", "15747670"))


        //
        //
        //        val response: HttpResponse[String] = Http("http://10.79.7.75:8088/v1/index/PtVectorSg/1672748956").asString
        //        println(response.body)
        //
        //
        //
        ////        val url = uri"http://10.79.7.75:8088/v1/index/PtVectorSg/1672748956"
        ////
        ////        val raw = Json.parse(
        ////            s"""
        ////               |{
        ////               |     "data_source": "com-sankuai-wmadrecall-hangu-admultirecall/ptU2ISkuEmb/20230103_195731",
        ////               |     "data_source_type": "s3",
        ////               |     "table_name": "PtVectorSg",
        ////               |     "schema": "com-sankuai-wmadrecall-hangu-admultirecall/sku_vector_pt.proto",
        ////               |     "output_type": "s3",
        ////               |     "output_path": "com-sankuai-wmadrecall-hangu-admultirecall",
        ////               |     "build_options": "forward.index.type=murmurhash,forward.index.hash.bucket.num=2097152, forward.segment.level=3,inverted.segment.level=6, inverted.term.index.type=murmurhash,inverted.term.index.hash.bucket.num=4194304",
        ////               |     "owner": "yangyufeng04"
        ////               |}
        ////               |""".stripMargin).toString()
        ////
        ////        val backend = HttpClientSyncBackend()
        ////        val response = basicRequest
        ////          .header("Content-Type", "application/Json")
        ////          .get(url)
        ////          .send(backend)
        ////        println(response.body.toString)
        //
        //    }
        //
        //    def geoHash(latitude: Int, longitude: Int): String = {
        //        val lat = (latitude * 1.0) / 1000000
        //        val lon = (longitude * 1.0) / 1000000
        //        val hash = GeoHash.withCharacterPrecision(lat, lon, 5)
        //        val base32 = hash.toBase32
        //        base32
        //    }
    }
}
