package waimai.job.local

import org.apache.spark.sql._
import waimai.utils.Json.JSONUtils.jsonObjectStrToMap
case class A(a: Option[Int], b: Option[Int], c: Option[Int])

object test {
    def main(args: Array[String]): Unit = {
//        val act = 3L
//        val entity = Map[String, String]()
//        var view_num = entity.getOrElse("view_num", "0").toInt
//        var click_num = entity.getOrElse("click_num", "0").toInt
//        var price = entity.getOrElse("price", "0").toDouble
//        act match {
//            case 3 => view_num += 1
//            case 2 => click_num += 1
//        }
//        println(view_num)
//        val a = jsonObjectStrToMap[String]("""{"value": 1}""")
//        val b = String.valueOf(a.getOrElse("value", "1")).toInt

         val row = Row(0L, 1.1d, null)
        // val a = row.getLong(0)        // error
        // val a = row.getString(0)      // error
        // val a = row.getAs[String](0)  // error
        // val a = row.getAs[Long](0)    // error
        // val b = String.valueOf(row.get(0))  // 不会报错，不确定是什么类型的时候使用， 如果是空值则返回 "null"
        // val b = row.get(1).toString   // error
        // val b = String.valueOf(row.get(1))  // "null"
        // val b = String.valueOf(row.get(0)).toLong  // 如果是double类型则会报错
        // val b = String.valueOf(row.get(0)).toDouble // 这么写基本不会报错
        // val b = row.getAs[Double](0)   // error Int 类型无法转换成double
        // val c = row.getAs[Long](1)        // error Double 类型无法转换成Long




        // val a = row.getLong(1)       // NullPointer error
        // val a = row.getAs[Long](1)   // 0
        // val b = row.isNullAt(0)      // false
        // val b = String.valueOf(row.get(1))  // null
        // val b = row.getAs[Option[Int]](1)      // 返回null 不要这么用
        // val b = row.get(1).asInstanceOf[Option[Int]]  // 返回null 不要这么用
        // val b = row.get(1).toString
        // 总结，不知道有没有小数
        //println(c)

//        val b = String.valueOf(row.get(0)).toDouble
//        if (b == 0) {               // java(String) 和 null 比较
//                                    // Int 和 0 比较
//                                    // Long 和 0 比较
//                                    // Integer(java类型) 和 null 比较
//            println(1)
//        }
//        println(a)
//        val a = String.valueOf(row.getAs[Long](0))
//        println((a + "," + "123" + a).split(",").mkString("Array(", ", ", ")"))
//        println((a + "," + "123" + a))
//        val b = "123"
////        println(b(0))
//        println(1+1)
    }
}
