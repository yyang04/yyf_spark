package utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag

object FileOperations {
    def saveAsTable(spark: SparkSession,
                    df: DataFrame,
                    tableName: String,
                    partition: Map[String, Any]): Unit = {

        val typeMap = Map("String" -> "string", "Integer" -> "int")
        val full_table_name = "mart_waimaiad.yyf04_" + tableName
        val schema = df.schema.map(x => x.name + " " + x.dataType.simpleString).mkString(",\n")
        val partitionString = partition.map { case (k, v) => k + " " + typeMap(v.getClass.getSimpleName) }.mkString(", ")

        spark.sql(
            s"""
                create table if not exists $full_table_name (
                    $schema
                ) partitioned by ($partitionString)
                STORED AS ORC
            """.stripMargin)

        val temp_input_data = "temp_input_data"
        df.createOrReplaceTempView(temp_input_data)
        val insertPartitionString = partition.map { case (k, v) => k + "=" + {
            if (v.getClass.getSimpleName == "Integer") {
                s"${v.toString}"
            } else {
                s"'${v.toString}'"
            }
        }
        }.mkString(", ")

        // insert sql
        spark.sql(
            s"""
              insert overwrite table $full_table_name partition ($insertPartitionString)
                select * from (
                    $temp_input_data
              )""".stripMargin)
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
}
