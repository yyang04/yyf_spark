package utils
import scala.math.{exp, log}

object ArrayOperations {
    // element-wise addition
    def add(x: Array[Double], y:Array[Double]): Array[Double] = {
        require(x.length == y.length)
        (x, y).zipped.map(_+_)
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
        val maxValue = x.max
        x.map(_/maxValue)
    }

    def logMaxScale(x: Array[Double]): Array[Double] = {
        x.max match {
            case 1.0 => div(x, 4.0)
            case _ =>
                val r = x.map(e => log(e+1))
                r.map(_/r.max)
        }
    }

    def main(args: Array[String]): Unit = {
        println(logMaxScale(Array(29.0,2.0)).mkString(","))
    }

}
