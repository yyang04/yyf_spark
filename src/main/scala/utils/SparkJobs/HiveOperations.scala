package utils.SparkJobs

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveOperations {
    def saveAsTable(spark:SparkSession,
                    df: DataFrame,
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
