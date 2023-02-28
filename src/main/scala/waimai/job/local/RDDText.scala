package waimai.job.local

import waimai.utils.SparkJobs.LocalSparkJob


object RDDText extends LocalSparkJob{

    override def run(): Unit = {



        sc.makeRDD(Seq(("20230203", "afasf", Array(1d,2d,3d))))
          .toDF("dt", "uuid", "user_emb")
          .write
          .partitionBy("dt")
          .text("data")

    }

}
