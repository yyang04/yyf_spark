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
        
    def kmeans_clustering()


        // Evaluate clustering by computing Within Set Sum of Squared Errors
        val WSSSE = clusters.computeCost(parsedData)
        println(s"Within Set Sum of Squared Errors = $WSSSE")

        // Save and load mode


//        // Load and parse the data
//        val data = sc.textFile("data/mllib/gmm_data.txt")
//        val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble))).cache()
//
//        // Cluster the data into two classes using GaussianMixture
//        val gmm = new GaussianMixture().setK(2).run(parsedData)
//
//        // Save and load model
//        gmm.save(sc, "target/org/apache/spark/GaussianMixtureExample/GaussianMixtureModel")
//        val sameModel = GaussianMixtureModel.load(sc,
//            "target/org/apache/spark/GaussianMixtureExample/GaussianMixtureModel")
//
//        // output parameters of max-likelihood model
//        for (i <- 0 until gmm.k) {
//            println("weight=%f\nmu=%s\nsigma=\n%s\n" format
//              (gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
//        }

    }
    def kmeans_clustering(data:RDD[Vector[Double]]): Unit ={



    }






}
