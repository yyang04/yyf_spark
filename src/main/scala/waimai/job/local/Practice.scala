package waimai.job.local

import waimai.utils.SparkJobs.LocalSparkJob
import scala.concurrent.duration._

object Practice {


    def main(args: Array[String]): Unit = {

        (1 to 10).foreach { x =>
            Thread.sleep(30.seconds.toMillis)
            println(x)
        }

    }
}
