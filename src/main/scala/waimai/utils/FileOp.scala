package waimai.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.duration._

import scala.util.control._
import scala.collection.mutable.Seq

object FileOp extends Serializable {
    // DF 写表操作，自动加前缀mart_waimaiad
    def saveAsTable(df: DataFrame, tableName: String, partition: Map[String, Any])(implicit spark: SparkSession): Unit = {
        val typeMap = Map("String" -> "string", "Integer" -> "int")
        val full_table_name = "mart_waimaiad." + tableName
        val schema = df.schema.map(x => x.name + " " + x.dataType.simpleString).mkString(",\n")
        val partitionString = partition.map { case (k, v) => k + " " + typeMap(v.getClass.getSimpleName) }.mkString(", ")
        val temp_input_data = "temp_input_data"
        df.createOrReplaceTempView(temp_input_data)

        val insertPartitionString = partition.map {
            case (k, v) => k + "=" +
              {
                  if (v.getClass.getSimpleName == "Integer") {
                      s"${v.toString}"
                  } else {
                      s"'${v.toString}'"
                  }
              }
        }.mkString(", ")

        val sql_create_table =
            s"""
               |create table if not exists $full_table_name (
               |$schema
               |)
               |partitioned by ($partitionString)
               |STORED AS ORC
               |""".stripMargin

        val sql_insert_table =
            s"""
               |insert overwrite table $full_table_name partition ($insertPartitionString)
               |    select * from ($temp_input_data)
               |""".stripMargin

        println(s"create sql: $sql_create_table")
        println(s"insert sql: $sql_insert_table")
        spark.sql(sql_create_table)
        spark.sql(sql_insert_table)
    }

    // RDD文件写入hdfs路径(覆盖), 写之前记得要 repartition
    def saveAsTextFile(rdd:RDD[_], path: String)(implicit hdfs: FileSystem): Unit = {
        val p = new Path(path)
        if (hdfs.exists(p)) hdfs.delete(p, true)
        rdd.saveAsTextFile(path)
    }

    // 将字符串写入hdfs文件，在driver端操作，是覆盖写
    def saveTextFile(data: Seq[String], path: String)(implicit hdfs: FileSystem): Unit = {
        val p = new Path(path)
        if (hdfs.exists(p)) hdfs.delete(p, true)
        val writer = HdfsOp.openHdfsFile(path, hdfs)
        for (line <- data) {
            HdfsOp.write[String](writer, line)
        }
        HdfsOp.closeHdfsFile(writer)
    }

    // 删掉hdfs文件，在driver端操作，如果存在就删除
    def deleteTextFile(path: String)(implicit hdfs: FileSystem): Unit = {
        val p = new Path(path)
        if (hdfs.exists(p)) hdfs.delete(p, true)
    }

    // 检验文件是否存在，默认5分钟检查一次，等待3个小时，如果存在则返回true，3个小时还不存在返回false
    def waitUntilFileExist(path: String, minuteStep: Int = 5, hourWait: Int = 3)(implicit hdfs: FileSystem): Boolean = {
        // 5分钟检测一次
        val maxTimes = (hourWait.hour / minuteStep.minute).round.toInt
        val loop = new Breaks
        loop.breakable{
            (0 until maxTimes).foreach { x =>
                val exist = hdfs.exists(new Path(path))
                if (exist) {
                    loop.break()
                    return true
                } else {
                    println(s"Try $x / $maxTimes")
                }
            }
        }
        false
    }
}
