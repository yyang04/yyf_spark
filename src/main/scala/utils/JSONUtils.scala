package utils
import play.api.libs.json._


object JSONUtils {
    def main(args: Array[String]): Unit = {
        val a = Map("lat" -> 51.235685, "long" -> -1.309197)
        val b = Json.toJson(a)
        val c = Json.toJson(
            Array(
                Json.obj(
                    "name" -> "Fiver",
                    "age" -> 4,
                    "role" -> JsNull
                ),
                Json.obj(
                    "name" -> "Bigwig",
                    "age" -> 6,
                    "role" -> "Owsla"
                )

            ))
        println(c.toString())

    }




}