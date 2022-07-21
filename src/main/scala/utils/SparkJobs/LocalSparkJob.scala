package utils.SparkJobs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

abstract class LocalSparkJob{
    Logger.getLogger("org").setLevel(Level.OFF)
    var conf: SparkConf = _
    var sc: SparkContext = _

    def initSpark(): Unit = {
        this.conf = new SparkConf()
          .setMaster("local")
          .setAppName(this.getClass.getName)
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        this.sc = new SparkContext(conf)
    }

    def main(args: Array[String]): Unit = {
        initSpark()
        run()
    }

    def run(): Unit = {}
}
