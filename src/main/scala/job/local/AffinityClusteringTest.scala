package job.local

import utils.SparkJobs.LocalSparkJob

object AffinityClusteringTest extends LocalSparkJob {
    def main(args: Array[String]): Unit = {
        super.initSpark()
    }
}
