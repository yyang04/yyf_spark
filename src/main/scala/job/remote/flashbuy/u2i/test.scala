package job.remote.flashbuy.u2i
import play.api.libs.json.{JsArray, JsNumber, Json}
object test {
    def main(args: Array[String]): Unit = {
        val emb = Array(1.0, 2.0).map(JsNumber(_))
        val poi = 123L
        val sku_id = "123"
        val tableName = "PtVectorSg"
        val utime = 1234435342L

        val emptyArray = Json.arr()
        emptyArray.append(JsNumber(10))

        val data = Json.obj(
            "embV1" -> JsArray(emb),
            "poiId" -> poi,
            "skuId" -> sku_id.toLong,
        ).toString

        val res = Json.obj(
            "table" -> tableName,
            "utime" -> utime,
            "data" -> data,
            "opType" -> 1,
            "pKey" -> sku_id
        ).toString
        println(res)
    }

}
