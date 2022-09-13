package job.local

import utils.SparkJobs.LocalSparkJob

object MyTest3 extends LocalSparkJob {
    override def run(): Unit = {
        val rdd = sc.textFile("data")
        val x = rdd.map(_.split("\t")).map(x => (x(0), x.takeRight(1)(0))).groupByKey.mapValues(x => x.toList.mkString(","))
        x.foreach(x => println(s"${x._1}\t${x._2}"))
//        x.collect.foreach(println(_))
//        val rdd = sc.parallelize(1 to 10)
//        rdd.saveAsTextFile("data2") //保存在本地

    }
}
