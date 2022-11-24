package utils.SparkJobs

import scopt.OptionParser
case class Config(beginDt: String = "",
                  endDt: String = "",
                  dt:String = "",
                  tableName: String = "",
                  sql: String = "",
                  threshold: Int = 50,
                  threshold2: Int = 2,
                  algorithm: String = "simhash",
                  timestamp: String = "",
                  hour:Int = 0,
                  city:String = "",
                  mode:String = "",
                  model_path:String = ""
                 )

class ArgsParser {
    val parser: OptionParser[Config] = new scopt.OptionParser[Config]("scopt") {
        head("scopt", "4.x")
        opt[String]("beginDt").action((x, c) => c.copy(beginDt = x))
        opt[String]("endDt").action((x, c) => c.copy(endDt = x))
        opt[String]("dt").action((x, c) => c.copy(dt = x))
        opt[String]("tableName").action((x, c) => c.copy(tableName = x))
        opt[String]("sql").action((x, c) => c.copy(sql = x))
        opt[Int]("threshold").action((x, c) => c.copy(threshold = x))
        opt[Int]("threshold2").action((x, c) => c.copy(threshold2 = x))
        opt[String]("algorithm").action((x, c) => c.copy(algorithm = x))
        opt[String]("timestamp").action((x, c) => c.copy(timestamp = x))
        opt[Int]("hour").action((x, c) => c.copy(hour = x))
        opt[String]("city").action((x, c) => c.copy(city = x))
        opt[String]("mode").action((x, c) => c.copy(mode = x))
        opt[String]("model_path").action((x, c) => c.copy(model_path = x))
    }

    def initParams(args: Array[String]):Config = {
        val defaultParams = Config()
        parser.parse(args, defaultParams) match {
            case Some(p) => p
            case None => defaultParams
        }
    }
}