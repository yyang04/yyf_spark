package utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

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
}
