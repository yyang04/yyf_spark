package mt.waimai

import mt.utils.SparkJob

object GhsClustering extends SparkJob {
    def main(args: Array[String]): Unit = {
        super.initSpark("GHSClustering", args)
        val count = spark.sql(
            """
              |select count(*) from mart_waimaiad.platinum_ctr_offline_feature_train_data_wc_zw180_murmur_newpage_life
              |where dt = 20211201
              |""".stripMargin).collect()
        println(count.mkString(""))
    }
}
