package waimai.job.local

import waimai.utils.SparkJobs.LocalSparkJob

object test4 extends LocalSparkJob {


	override def run(): Unit = {
		// spark.udf.register("spu_sort", spu_sort(_: Array[String], _:String))
		val result = spark.sql(
			s"""
			   |select 1
			   |""".stripMargin).collect()
		println(1)



	}

	def spu_sort(spu_info_list: Array[String], split_flag: String): Seq[String] = {
		spu_info_list.sortBy(x => 10000 * x.split(split_flag)(3).toDouble - x.split(split_flag)(4).toDouble).reverse.take(5)
	}





}
