package utils.SparkJobs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

abstract class LocalSparkJob extends SQLImplicits with Serializable {
    Logger.getLogger("org").setLevel(Level.OFF)
    var conf: SparkConf = _
    implicit var sc: SparkContext = _
    var spark : SparkSession = _
    override var _sqlContext: SQLContext = _

    def initSpark(): Unit = {
        this.conf = new SparkConf()
          .setMaster("local")
          .setAppName(this.getClass.getName)
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.sql.catalogImplementation","hive")
        this.spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate
        // this.spark = SparkSession.builder().config(conf).getOrCreate
        this.sc = spark.sparkContext
        this._sqlContext = this.spark.sqlContext
    }

    def main(args: Array[String]): Unit = {
        initSpark()
        run()
    }

    def run(): Unit = {}
}
