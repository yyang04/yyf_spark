package waimai.utils

import scala.math.{exp, log}
import scala.reflect.ClassTag
import scala.util.Random

object ArrayOp {
    // element-wise addition
    def add(x: Array[Double], y:Array[Double]): Array[Double] = {
        require(x.length == y.length)
        (x, y).zipped.map(_+_)
    }
    def add_2(x: Array[(Double, Double)], y: Array[(Double, Double)]): Array[(Double, Double)] ={
        require(x.length == y.length)
        (x, y).zipped.map{
            case (x, y) =>
                (x._1 + y._1, x._2 + y._2)
        }
    }

    def div(x: Array[Double], y: Double): Array[Double] = {
        x.map(_/y)
    }

    // dot Product
    def dotProduct(x: Array[Double], y: Array[Double]): Double = {
        require(x.length == y.length)
        (for ((a, b) <- x zip y) yield a * b).sum
    }

    // l2 norm
    def norm(x: Array[Double]): Double = {
        math.sqrt(x.map(i => i * i).sum)
    }

    // cosine similarity
    def cosineSimilarity(x: Array[Double], y: Array[Double]): Double = {
        require(x.length == y.length)
        dotProduct(x, y) / (norm(x) * norm(y))
    }

    // l2 distance
    def distance(x: Array[Double], y: Array[Double]): Double = {
        require(x.length == y.length)
        norm((x, y).zipped.map(_ - _))
    }

    def softmax(x: Array[Double]): Array[Double] = {
        val tmp = x.map(e => exp(e - x.max))
        tmp.map(e => e / (tmp.sum + 1e-16))
    }

    def maxScale(x: Array[Double]): Array[Double] ={
        val y = x.map(_ - x.min + 1)
        y.map(_/y.max)
    }

    def logMaxScale(x: Array[Double]): Array[Double] = {
        x.max match {
            case 1.0 => div(x, 4.0)
            case _ =>
                val r = x.map(e => log(e+1))
                r.map(_/r.max)
        }
    }

    def randomChoice[T: ClassTag](x: Array[T], n: Int): Array[T] = {
        Random.shuffle(x.toList).take(n).toArray
    }

    def entropy(x: Array[Double]): Double = {
        val total = x.sum
        val res = x.map(_/total)
        res.map{x => - x * math.log(x)}.sum
    }



    def main(args: Array[String]): Unit = {
//        println(maxScale(Array(2.0,3.0)).mkString)
//        val a = Array((1d,2d),(3d,4d))
//        val b = Array((2d,3d),(3d,5d))
//        println(add_2(a, b).mkString(","))
//        val a = Array(1d, 2d, 3d, 4d)
//        val b = Array(1d, 1d, 1d, 1d)
//        val c = Array(1d, 1d, 1d, 1d, 1d)
//        println(entropy(a))
//        println(entropy(b))
//        println(entropy(c))
//        val r = new Random
//        val data = Array(1, 2, 3, 4, 5)
//        val weights = Array(1.0, 2.0, 3.0, 4.0, 5.0)
//        val result = weightedSampleWithReplacement[Int](data, weights, 5, random=r)
        val result = randomChoice(Array(1,2,3), 1)
        println(result.mkString(","))

    }





}
