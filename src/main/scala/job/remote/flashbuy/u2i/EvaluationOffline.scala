package job.remote.flashbuy.u2i
import sttp.client3._
import play.api.libs.json.{JsArray, JsNumber, Json}

import scala.util.control.Breaks


object EvaluationOffline {
    def main(args: Array[String]): Unit = {
        val loop = new Breaks
        loop.breakable {
            for (i <- 1 to 10) {
                for (j <- 1 to 10) {
                    println(i)
                    if (i == 2) {
                        loop.break()
                    }
                }
            }
        }

    }
}
