package waimai.utils.SparkJobs

import scopt.OptionParser
import waimai.utils.DateOp
case class Config(beginDt: String = "",
                  endDt: String = "",
                  dt: String = DateOp.getNDaysAgo(1), // 如果不传参数自动取昨天
                  algorithm: String = "",
                  timestamp: String = "",
                  tableName: String = "",
                  methodName: String = "",
                  branch: String = "",
                  src_table_name: String = "",
                  dst_table_name: String = "",
                  sql: String = "",
                  city:String = "",
                  mode:String = "",
                  model_path:String = "",
                  version: String = "",
                  config: String = "",
                  threshold: Int = 0,
                  threshold1: Int = 0,
                  threshold2: Int = 0,
                  hour: Int = 0,
                  window: Int = 0,
                  item_embedding_path: String = "",
                  user_embedding_path: String = ""
                 )

class ArgsParser {
    val parser: OptionParser[Config] = new scopt.OptionParser[Config]("scopt") {
        head("scopt", "4.x")
        opt[String]("beginDt").action((x, c) => c.copy(beginDt = x))
        opt[String]("endDt").action((x, c) => c.copy(endDt = x))
        opt[String]("dt").action((x, c) => c.copy(dt = x))
        opt[String]("tableName").action((x, c) => c.copy(tableName = x))
        opt[String]("sql").action((x, c) => c.copy(sql = x))
        opt[String]("algorithm").action((x, c) => c.copy(algorithm = x))
        opt[String]("timestamp").action((x, c) => c.copy(timestamp = x))
        opt[String]("city").action((x, c) => c.copy(city = x))
        opt[String]("mode").action((x, c) => c.copy(mode = x))
        opt[String]("model_path").action((x, c) => c.copy(model_path = x))
        opt[String]("src_table_name").action((x, c) => c.copy(src_table_name = x))
        opt[String]("dst_table_name").action((x, c) => c.copy(dst_table_name = x))
        opt[String]("version").action((x, c) => c.copy(version = x))
        opt[String]("config").action((x, c) => c.copy(config = x))
        opt[String]("methodName").action((x, c) => c.copy(config = x))
        opt[String]("branch").action((x, c) => c.copy(config = x))
        opt[String]("item_embedding_path").action((x, c) => c.copy(config = x))
        opt[String]("user_embedding_path").action((x, c) => c.copy(config = x))
        opt[Int]("threshold").action((x, c) => c.copy(threshold = x))
        opt[Int]("threshold1").action((x, c) => c.copy(threshold1 = x))
        opt[Int]("threshold2").action((x, c) => c.copy(threshold2 = x))
        opt[Int]("hour").action((x, c) => c.copy(hour = x))
        opt[Int]("window").action((x, c) => c.copy(window = x))
    }

    def initParams(args: Array[String]):Config = {
        val defaultParams = Config()
        parser.parse(args, defaultParams) match {
            case Some(p) => println(p); p
            case None => defaultParams
        }
    }
}