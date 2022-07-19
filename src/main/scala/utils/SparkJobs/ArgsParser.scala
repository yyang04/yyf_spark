package utils.SparkJobs

import scopt.OptionParser
case class Config(beginDt: String = "",
                  endDt: String = "")

class ArgsParser {
    val parser: OptionParser[Config] = new scopt.OptionParser[Config]("scopt") {
        head("scopt", "4.x")
        opt[String]("beginDt").action((x, c) => c.copy(beginDt = x))
        opt[String]("endDt").action((x, c) => c.copy(endDt = x))
    }

    def initParams(args: Array[String]):Config = {
        val defaultParams = Config()
        parser.parse(args, defaultParams) match {
            case Some(p) => p
            case None => defaultParams
        }
    }
}