package waimai.job.local
import scalaj.http.Http
import waimai.utils.JsonOp

object test1 {


	def main(args: Array[String]): Unit = {
		val url = """http://ppop.waimai.st.sankuai.com/productTair/batchGetSkuBaseInfo?skuIds=17505051001,17511113663,17505050982,17511113635,17511385267,17505050991,17505051026,17505051010,17505051049,17511113629,14414597715,9562632456,11106008014,13387240084,14414103841,8840269407,13070362387,13387240108,13208584123,8805841340"""
		val result = Http(url).header("content-type", "application/json").asString.body

		val a = JsonOp.jsonObjectStrToArrayMap(result)

		for (element <- a) {

			val sell_status = element.getOrElse("sell_status", 1)
			val product_status = element.getOrElse("product_status", 1)
			val product_id = element.get("id").get.toString
			println(s"product_id: $product_id, sell_status: $sell_status, product_status: $product_status")
		}

	}

}
