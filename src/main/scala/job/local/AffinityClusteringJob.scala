package job.local

import org.apache.spark.ml.linalg.{DenseMatrix, Vectors}
import org.apache.spark.rdd.RDD
import utils.SparkJobs.LocalSparkJob
import utils.PrivacyClustering.{AffinityClustering, EdgeAttr, Neighbor, VertexAttr}

import java.util.{Random => JR}
import breeze.linalg.{*, Axis, norm, DenseMatrix => BDM, DenseVector => BDV}
//import com.linkedin.nn.algorithm.L2ScalarRandomProjectionNNS
import utils.ArrayOperations.distance


object AffinityClusteringJob extends LocalSparkJob {
//    override def run(): Unit = {
//        val rows = 20
//        val cols = 10000
//        val seed = 3
//        val v = new BDM(rows, cols, DenseMatrix.randn(rows, cols, new JR(seed)).toArray)
//        val data = sc.parallelize(for (i <- 0 until cols) yield v(::, i).toArray)
//          .zipWithIndex
//          .map(_.swap)
//          .map{ case (id, embed) => (id, Vectors.dense(embed)) }
//
//
//        val model = new L2ScalarRandomProjectionNNS()
//          .setNumHashes(300)
//          .setSignatureLength(15)
//          .setBucketLimit(1000)
//          .setBucketWidth(20)
//          .setShouldSampleBuckets(true)
//          .setJoinParallelism(20)
//          .setNumOutputPartitions(20)
//          .createModel(dimension=20)
//
//        val edges = model.getSelfAllNearestNeighbors(data, 5)
//
//        val tmp = data.map{ case(id, emb) => (id, emb.toArray) }
//        val tmp2 = tmp.collect
//        val bf = tmp.map{ case (id, emb) =>
//            val res = tmp2.map{ case (id2, emb2) => (id2, distance(emb, emb2)) }.sortBy(_._2).take(5).map(_._1)
//            (id, res)
//        }
//
//        val ann = edges.map{ case(src, dst, w) => (src, dst) }.groupByKey.mapValues(iter => iter.toList)
//        ann.collect.foreach(println(_))
//
//        val res = bf.join(ann).map{ case(id, (list1, list2)) => (list1.intersect(list2).length / 5.0, 1)}.reduce((x,y) => (x._1 + y._1, x._2 + y._2))
//        println(res._1 / res._2)
//
//
//    }
}