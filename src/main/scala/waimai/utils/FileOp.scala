package waimai.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.duration._

import scala.util.control._
import scala.collection.mutable.Seq

object FileOp extends Serializable {
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

    def waitUntilFileExist(hdfs: FileSystem, path: String, minuteStep: Int=5, hourWait: Int=3): Boolean = {
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

    class GenericRowWithSchemaWithOptionalField(values: Array[Any], override val schema: StructType)
      extends GenericRowWithSchema(values, schema) {
        def getOpt[T](fieldname: String): Option[T] = {
            if (isNullAt(fieldIndex(fieldname))) None
            else Some(getAs[T](fieldname))
        }
    }
}
