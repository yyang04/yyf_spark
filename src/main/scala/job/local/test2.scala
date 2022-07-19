package job.local

import breeze.linalg.{DenseMatrix, DenseVector}
import utils.SparkJobs.LocalSparkJob

object test2 extends LocalSparkJob {
    def main(args: Array[String]): Unit = {
        super.initSpark()
//        val a = List(1,2,3)
//        a.min(new Ordering[Int](){
//            override def compare(x: Int, y: Int): Int = {
//                Ordering[Int].compare(x,y)
//            }
//        })
        val eye = DenseMatrix.eye[Double](4)
        val eye2 = DenseMatrix.eye[Double](4)
        val x = DenseVector(1.0, 2.0, 2.0, 3.0)
        val res = (x.t * eye)



    }
}
