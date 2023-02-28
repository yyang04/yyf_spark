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
    def sampleWeightedRDD[T: ClassTag](ar: RDD[Sample[T]], numSamples: Int)(implicit sc: SparkContext): RDD[T] = {
        // 1. Get total weight on each partition
        var partitionWeights = ar.mapPartitionsWithIndex { case (partitionIndex, iter) => Array((partitionIndex, iter.map(_.weight).sum)).toIterator }.collect()

        // Normalize to 1.0
        val Z = partitionWeights.map(_._2).sum
        partitionWeights = partitionWeights.map { case (partitionIndex, weight) => (partitionIndex, weight / Z) }

        // 2. Sample from partitions indexes to determine number of samples from each partition
        val bcSamplesPerIndex = sc.broadcast(sample[Int](partitionWeights, numSamples).groupBy(x => x).map(x => (x._1, x._2.length)))

        // 3. On each partition, sample the number of elements needed for that partition
        ar.mapPartitionsWithIndex { case (partitionIndex, iter) =>
            val samplesPerIndex = bcSamplesPerIndex.value
            val numSamplesForPartition = samplesPerIndex.getOrElse(partitionIndex, 0)
            var ar = iter.map(x => (x.obj, x.weight)).toArray
            val Z = ar.map(x => x._2).sum
            ar = ar.map { case (obj, weight) => (obj, weight / Z) }
            sample(ar, numSamplesForPartition).toIterator
        }
    }

    def sample[T: ClassTag](dist: Array[(T, Double)], numSamples: Int): Array[T] = {

        val probs = dist.zipWithIndex.map { case ((elem, prob), idx) => (elem, prob, idx + 1) }.sortBy(-_._2)
        val cumulativeDist = probs.map(_._2).scanLeft(0.0)(_ + _).drop(1)
        (1 to numSamples).toArray.map(_ => scala.util.Random.nextDouble).map { p =>
            def findElem(p: Double, cumulativeDist: Array[Double]): Int = {
                for (i <- 0 until cumulativeDist.length - 1)
                    if (p <= cumulativeDist(i)) return i
                cumulativeDist.length - 1
            }

            probs(findElem(p, cumulativeDist))._1
        }
    }

    def weightedSampleWithReplacement[T: ClassTag](ar: Array[Sample[T]],
                                                   n: Int,
                                                   random: Random): Array[T] = {
        val cumWeights = ar.map(_.weight).scanLeft(0.0)(_ + _)
        val cumProbs = cumWeights.map(_ / cumWeights.last)
        Array.fill[T](n) {
            val r = random.nextDouble()
            ar.map(_.obj).apply(cumProbs.indexWhere(r < _) - 1)
        }
    }


}


