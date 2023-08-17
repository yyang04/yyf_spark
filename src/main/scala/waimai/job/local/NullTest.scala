package waimai.job.local

import org.apache.spark.sql.functions.lit
import waimai.utils.SparkJobs.LocalSparkJob

case class Person(name: String, age: Integer) {
    val fullName: String = name + age.toString
}
case class FullPerson(name: String, age: Integer, gender: String)

object NullTest extends LocalSparkJob {

    override def run(): Unit = {
        val df = Seq(("Alice", 25), ("Bob", 33)).toDF("name", "age").as[Person]
        df.map(x=> x.fullName)
        // df.withColumn("year", lit(2021)).write.mode("overwrite").partitionBy("year").format("orc").saveAsTable("my_hive_table")

//        spark.sql(
//            """
//              |select name, age
//              |  from my_hive_table
//              |""".stripMargin).as[Person].collect.foreach(println(_))


    }
}
