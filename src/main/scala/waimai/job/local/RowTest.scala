package waimai.job.local

import org.apache.spark.sql._

object RowTest {
    def main(args: Array[String]): Unit = {

         val row = Row(null, 0L, 1.1d, "1", 0, false)
        // 空值问题
        // val a = row.getLong(0)             // error
        // val a = row.getString(0)           // error
        // val a = row.get(0).toString        // error
        // val a = row.getAs[Long](0)         // return 0
        // val a = row.getAs[Double](0)       // return 0.0
        // val a = row.getAs[Boolean](0)      // return false
        // val a = row.getAs[String](0)       // return null
        // val a = String.valueOf(row.get(0)) // return "null"
        // val a = row.isNullAt(0)            // true

        // 类型转换问题
        // val a = row.getAs[Double](1)       // error Long类型无法转换成double
        // val a = row.getAs[Long](2)         // error Double类型无法转换成Long
        // val a = row.getAs[String](1)       // error Long类型无法转换成String
        // val a = row.getAs[Long](4)         // error Int类型无法转换成Long
        // val a = String.valueOf(row.get(1)) // 万能方法，可以返回字符串,分别是0, 1.1, 1, 0, false
    }
}
