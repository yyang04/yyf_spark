package waimai.job.remote.flashbuy.evaluation

import scalaj.http.Http
import play.api.libs.json.Json
import waimai.utils.SparkJobs.RemoteSparkJob

object DailyMonitor {
    def main(args: Array[String]): Unit = {
        val url = "http://tsp-openapi.vip.sankuai.com/api/abgetAllWmExpGroupsByScene"

        val data = Json.parse(
            s"""
               |{
               |    "sceneKey":"multirecall_layer_data"
               |}
               |""".stripMargin)
          .toString
        val response = Http(url).postData(data).header("content-type", "application/json").asString.body
        println(response)
    }
}
