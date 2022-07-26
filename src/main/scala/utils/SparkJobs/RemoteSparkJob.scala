package utils.SparkJobs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

abstract class RemoteSparkJob extends ArgsParser with SQLImplicits with Serializable {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var spark: SparkSession = _
    var sc: SparkContext = _
    var params: Config = Config()
    override var _sqlContext: SQLContext = _

    def main(args: Array[String]): Unit ={
        initSpark(args)
        run()
    }

    def run(): Unit ={}

    def initSpark(args: Array[String]): Unit ={
        this.params = super.initParams(args)
        val conf = {
            new SparkConf()
              .setAppName(this.getClass.getName)
              .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")  // 优化应用序列化（使用Kryo）
              .set("spark.kryoserializer.buffer.max", "512m")                        // 默认64 Kryo序列化缓存允许的最大值。这个值必须大于你尝试序列化的对象
              .set("hive.exec.dynamic.partition", "true")                            // 是否允许动态生成分区
              .set("hive.exec.dynamic.partition.mode", "nonstrict")                  // 是否容忍指定分区全部动态生成
              .set("spark.sql.adaptive.enabled", "true")                             // 默认开启，是否开启调整partition功能
              .set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128000000")  // 不太清楚
              .set("spark.sql.autoBroadcastJoinThreshold", "200000000")              // 默认10M，这里设置200M
              .set("spark.sql.hive.metastorePartitionPruning", "true")               // Hive表为ORC模式时，官方建议加的
              .set("spark.shuffle.service.enabled", "true")
              .set("spark.input.dir.recursive","true")
              .set("spark.sql.ignore.existed.function.enable","true")
              .set("spark.yarn.executor.memoryOverhead","1024")
              .set("spark.yarn.driver.memoryOverhead","1024")
              .set("spark.hadoop.hive.mt.renew.token.enable", "true")
              .set("spark.hadoop.hive.mt.renew.token.enable", "true")
              .set("spark.driver.maxResultSize","12G")
        }
        this.spark = SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport.getOrCreate

        this._sqlContext = this.spark.sqlContext
        val confMap = spark.sparkContext.getConf.getAll.toMap
        val core = confMap.getOrElse("spark.executor.cores", "4").toInt
        val executor = confMap.getOrElse("spark.executor.instances", confMap.getOrElse("spark.dynamicAllocation.maxExecutors", "600")).toInt
        var cores = executor * core
        if (cores % 10 > 0) {
            cores = (cores / 10 + 1) * 10
        }
        val parallelism = cores * 2
        spark.conf.set("spark.sql.shuffle.partitions", parallelism)
        spark.conf.set("spark.default.parallelism", parallelism)
        this.sc = spark.sparkContext
        this.sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    }


    def saveAsTable(df: DataFrame,
                    tableName: String,
                    partition: Map[String, Any]): Unit = {

        val typeMap = Map("String" -> "string", "Integer" -> "int")
        val full_table_name = "mart.waimaiad.yyf04_" + tableName
        val schema = df.schema.map(x => x.name + " " + x.dataType.simpleString).mkString(",\n")
        val partitionString = partition.map{ case (k,v) => k + " " + typeMap(v.getClass.getSimpleName)}.mkString(", ")

        // insert sql
        spark.sql(s"""
                create table if not exists $full_table_name (
                    $schema
                ) partitioned by ($partitionString)
                STORED AS ORC
            """.stripMargin)

        val temp_input_data = "temp_input_data"
        df.createOrReplaceTempView(temp_input_data)

        val insertPartitionString = partition.map{ case (k,v) => k + "=" + {
            if (v.getClass.getSimpleName == "Integer") {
                s"${v.toString}"
            } else {
                s"'${v.toString}'"
            }
        }}.mkString(", ")

        // insert sql
        spark.sql(s"""
              insert overwrite table $full_table_name partition ($insertPartitionString)
                select * from (
                    $temp_input_data
              )""".stripMargin)
    }

}


