package utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.control._

import scala.reflect.ClassTag

object FileOperations extends Serializable {
    def saveAsTable(spark: SparkSession,
                    df: DataFrame,
                    tableName: String,
                    partition: Map[String, Any]): Unit = {

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


    def persistRDD[T: ClassTag](sc: SparkContext,
                                hdfs: FileSystem,
                                rdd: RDD[T],
                                path: String): RDD[T] = {

        val full_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/resys/${sc.applicationId}/$path"
        val p = new Path(full_path)
        if (hdfs.exists(p)) hdfs.delete(p, true)
        rdd.saveAsObjectFile(full_path)
        sc.objectFile[T](full_path)
    }

    def saveAsTextFile(hdfs: FileSystem,
                       rdd:RDD[_],
                       path: String): Unit ={
        val p = new Path(path)
        if (hdfs.exists(p)) hdfs.delete(p, true)
        rdd.saveAsTextFile(path)
    }

    def saveTextFile(hdfs: FileSystem,
                     data: Seq[String],
                     path: String
                     ): Unit ={
        val p = new Path(path)
        if (hdfs.exists(p)) hdfs.delete(p, true)
        val writer = HdfsOp.openHdfsFile(path, hdfs)
        for (line <- data) {
            HdfsOp.write[String](writer, line)
        }
        HdfsOp.closeHdfsFile(writer)
    }

    def deleteTextFile(hdfs: FileSystem,
                       path: String) : Unit ={
        val p = new Path(path)
        if (hdfs.exists(p)) hdfs.delete(p, true)
    }


    def parseSchema(schema: String): StructType = {
        val type_map = Map("int" -> IntegerType, "string" -> StringType, "bigint" -> LongType, "double" -> DoubleType)
        val finalSchema = StructType(
            schema
              .split("\n")
              .map(x => {
                  val arr = x.split(" ")
                  StructField(arr(0), type_map(arr(1)), nullable=true)
        }))
        finalSchema
    }

    def waitUntilFileExist(hdfs: FileSystem,
                           path: String,
                           interval: Int = 5,
                           maxTimes: Int = 180 / 5): Boolean = {
        var index = 0
        val loop = new Breaks
        loop.breakable {
            while (index < maxTimes) {
                val exist = hdfs.exists(new Path(path))
                if (exist) {
                    loop.break
                }
                Thread.sleep(interval * 1000 * 60)
                index = index + 1
            }
        }
        index != maxTimes  // 存在返回 True, 不存在返回 false
    }
}
