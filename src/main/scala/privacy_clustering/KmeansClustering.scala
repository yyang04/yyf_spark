package privacy_clustering
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.rdd.RDD
import utils.SparkJob

object KmeansClustering extends SparkJob {
    def main(args: Array[String]): Unit = {
        super.initSpark(this.getClass.getName, args)

        val data = sc.textFile("data/mllib/kmeans_data.txt")
        val parsedData: RDD[Vector] = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

        val numClusters = 2
        val numIterations = 20
        val clusters = KMeans.train(parsedData, 1, 10)
        clusters.predict(parsedData)
    }
}
