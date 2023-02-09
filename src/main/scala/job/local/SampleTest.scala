package job.local

import job.remote.flashbuy.u2i.sample.ModelSample
import utils.SampleOperations
import utils.SparkJobs.LocalSparkJob
import utils.Sample


object SampleTest extends LocalSparkJob{

    def f(x: Double): Double =  { (math.sqrt(x / 0.001) + 1) * 0.001 / x}
    def f2(x: Double): Double = math.pow(x, -0.5)
    override def run(): Unit = {


        val a = sc.makeRDD((1 to 1000).flatMap(x => Array.fill[Int](x)(x)), 5)
        val total = a.count.toInt
        val b = a.map{ x => Sample( f(x/total.toDouble), x)}

        val sample_sku_pos = SampleOperations.sampleWeightedRDD[Int](b, total)
        println(sample_sku_pos.collect.groupBy(identity).mapValues(_.length).toArray.sortBy(_._1).mkString(","))

    }

}
