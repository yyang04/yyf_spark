package job.local

import utils.SparkJobs.LocalSparkJob

import scala.util.parsing.json._

object MyTest5 extends LocalSparkJob {
    override def run(): Unit = {
        val m = Map(1->2, 2->3)
        val a = Array.tabulate(10)(x => x)
        a.flatMap(x=> m.get(x)).foreach(println(_))
    }
}
