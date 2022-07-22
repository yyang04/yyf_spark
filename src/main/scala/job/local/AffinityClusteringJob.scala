package job.local

import org.apache.spark.ml.linalg.{DenseMatrix, Vectors}
import org.apache.spark.rdd.RDD
import utils.SparkJobs.LocalSparkJob
import utils.privacy_clustering.{AffinityClustering, EdgeAttr, Neighbor, VertexAttr}

import java.util.{Random => JR}
import breeze.linalg.{*, Axis, norm, DenseMatrix => BDM, DenseVector => BDV}
import utils.privacy_clustering.Metrics.evaluate_minkowski
import scala.util.control.Breaks

object AffinityClusteringJob extends LocalSparkJob {
    override def run(): Unit = {
        val v = new BDM(2, 100, DenseMatrix.randn(2, 100, new JR(3)).toArray)
        val data = sc.parallelize(for (i <- 0 until 100) yield v(::, i).toArray).zipWithIndex.map(x => (x._2.toString, x._1))
        val model = new AffinityClustering(
            upperBound = 20,
            lowerBound = 5,
            threshold = 5,
            numHashes = 2,
            signatureLength = 1,
            joinParallelism = 5,
            bucketLimit = 10,
            bucketWidth = 1,
            outputPartitions = 5,
            num_neighbors = 10,
            num_steps = 3
        )
        val result = model.fit(sc, data)
        println(evaluate_minkowski(result.map(x => (x._2, x._3))))
    }
}