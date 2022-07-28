package utils

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
}
