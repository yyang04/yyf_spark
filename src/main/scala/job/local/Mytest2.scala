package job.local

import utils.SparkJobs.LocalSparkJob
import utils.TimeOperations

object Mytest2 extends LocalSparkJob {

    override def run(): Unit = {
        val res = TimeOperations.getTimestamp("20211120")
        println(res)

    }

    def a(): Long = {
        1
    }
}
