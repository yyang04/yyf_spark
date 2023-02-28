package waimai.job.local

import waimai.utils.SparkJobs.LocalSparkJob
//import scala.util.parsing.Json._

object MyTest4 extends LocalSparkJob {
//    override def run(): Unit = {
//        val parsed = JSON.parseFull(
//            """[{"recommend_sku_infos":[{"dup_sparse_weight":0,"first_category_id":200002498,"id":10767799755,"second_category_id":200002501,"spu_id":8362026523,"third_category_id":200002612},
//              |{"dup_sparse_weight":0,"first_category_id":200002498,"id":10767799235,"second_category_id":200002506,"spu_id":8362026238,"third_category_id":200002558}]}]""".stripMargin)
//          .get.asInstanceOf[List[Map[String, Any]]]
//          .head("recommend_sku_infos")
//          .asInstanceOf[List[Map[String, Double]]]
//        val result = parsed.map(x => (x("spu_id").toLong, x("third_category_id").toLong)).toMap
//        print(result)
////        println(parsed)
//
//
//    }
}
