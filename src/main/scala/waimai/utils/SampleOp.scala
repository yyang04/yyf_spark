package waimai.utils

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.util.Random


case class Sample[T](weight: Double, obj: T)

/*
 *  Obtain a sample of size `numSamples` from an RDD `ar` using a two-phase distributed sampling approach.
 */

object SampleOperations {
    // 分布式重要性采样，n为采样个数
    def sampleWeightedRDDWithReplacement[T: ClassTag](ar: RDD[Sample[T]], n: Int)(implicit sc: SparkContext): RDD[T] = {
        // 1. Get total weight on each partition
        var partitionWeights = ar.mapPartitionsWithIndex { case (partitionIndex, iter) => Array(Sample(iter.map(_.weight).sum, partitionIndex)).toIterator }.collect()

        // 2. Sample from partitions indexes to determine number of samples from each partition
        val bcSamplesPerIndex = sc.broadcast(weightedSampleWithReplacement[Int](partitionWeights, n).groupBy(x => x).map(x => (x._1, x._2.length)))

        // 3. On each partition, sample the number of elements needed for that partition
        ar.mapPartitionsWithIndex { case (partitionIndex, iter) =>
            val samplesPerIndex = bcSamplesPerIndex.value
            val numSamplesForPartition = samplesPerIndex.getOrElse(partitionIndex, 0)
            weightedSampleWithReplacement[T](iter.toArray, numSamplesForPartition).toIterator
        }
    }

    // 单机重要性采样, n为采样个数
    // import scala.util.Random
    // SampleOp.weightedSampleWithReplacement(ar, n)
    def weightedSampleWithReplacement[T: ClassTag](ar: Array[Sample[T]], n: Int): Array[T] = {
        val random = new Random
        val cumWeights = ar.map(_.weight).scanLeft(0.0)(_ + _)
        val cumProbs = cumWeights.map(_ / cumWeights.last)
        Array.fill[T](n) {
            val r = random.nextDouble()
            ar.map(_.obj).apply(cumProbs.indexWhere(r < _) - 1)
        }
    }
}


