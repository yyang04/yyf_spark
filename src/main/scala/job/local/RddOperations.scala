package job.local

import utils.SparkJobs.LocalSparkJob

object RddOperations extends LocalSparkJob{
    override def run(): Unit = {
        val rdd = sc.textFile("data").collect()
        rdd.foreach(println(_))

        // co-group
        // subtract by key
        // fold by key
        // left outer join
        // sc.setCheckpointDir("")

    }

}
