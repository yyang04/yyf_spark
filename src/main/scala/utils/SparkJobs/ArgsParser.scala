package utils.SparkJobs

import scopt.OptionParser
case class Config(beginDt: String = "",
                  endDt: String = "",
                  tableName: String = "",
                  sql: String = "",
                  threshold: Int = 2000,
                  algorithm: String = "simhash",
                  timestamp: String = "",
                  dt:String = "",
                  hour:Int = 0)

class ArgsParser {
    val parser: OptionParser[Config] = new scopt.OptionParser[Config]("scopt") {
        head("scopt", "4.x")
        opt[String]("beginDt").action((x, c) => c.copy(beginDt = x))
        opt[String]("endDt").action((x, c) => c.copy(endDt = x))
        opt[String]("tableName").action((x, c) => c.copy(tableName = x))
        opt[String]("sql").action((x, c) => c.copy(sql = x))
        opt[String]("algorithm").action((x, c) => c.copy(algorithm = x))
        opt[Int]("threshold").action((x, c) => c.copy(threshold = x))
        opt[String]("timestamp").action((x, c) => c.copy(timestamp = x))
        opt[String]("dt").action((x, c) => c.copy(dt = x))
        opt[Int]("hour").action((x, c) => c.copy(hour = x))
    }

    def initParams(args: Array[String]):Config = {
        val defaultParams = Config()
        parser.parse(args, defaultParams) match {
            case Some(p) => p
            case None => defaultParams
        }
    }
}