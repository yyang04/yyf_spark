package waimai.job.remote.flashbuy.recall.evaluation

import waimai.utils.ArrayOp

class RecallEvaluation (rel:Set[Long], predict: Array[Long], k: Array[Int]) {
    var count: Double = 1d

    var hit_rate: Array[Double] = {
        k.map { x =>
            val predict_k = predict.take(x).toSet
            rel.intersect(predict_k).size match {
                case x if x > 0 => 1d
                case _ => 0d
            }
        }
    }

    var precision: Array[(Double, Double)] = {
        k.map{ x =>
            val predict_k = predict.take(x).toSet
            (rel.intersect(predict_k).size.toDouble, predict_k.size.toDouble)
        }
    }

    var recall: Array[(Double, Double)] = {
        k.map { x =>
            val predict_k = predict.take(x).toSet
            (rel.intersect(predict_k).size.toDouble, rel.size.toDouble)
        }
    }

    var ndcg_k: Array[Double] = {
        k.map { x =>
            val predict_k = predict.take(x)
            ndcg(rel, predict_k)
        }
    }

    def ndcg(rel: Set[Long],
             predict_k: Array[Long]): Double = {

        val dcg = predict_k.map{ x => if (rel contains x) 1d else 0d }
          .zipWithIndex.map { case (x, index) => x / (scala.math.log(index + 2) / scala.math.log(2))
        }.sum
        val idcg = predict_k.map { x => if (rel contains x) 1d else 0d }.sorted.reverse.zipWithIndex.map {
            case (x, index) => x / (scala.math.log(index + 2) / scala.math.log(2))
        }.sum
        if (idcg == 0) 0d else dcg / idcg
    }

    def union(e: RecallEvaluation) : RecallEvaluation = {
        this.hit_rate = ArrayOp.add(this.hit_rate, e.hit_rate)
        this.precision = ArrayOp.add_2(this.precision, e.precision)
        this.recall = ArrayOp.add_2(this.recall, e.recall)
        this.ndcg_k = ArrayOp.add(this.ndcg_k, e.ndcg_k)
        this.count = this.count + e.count
        this
    }

    def print_entity(): Unit = {
        println(s"Total User: $count")
        k.zipWithIndex.foreach{
            case(x, index) =>
                println(f"HR@$x: ${hit_rate(index)/count * 100}%.2f%%")
                println(f"Precision@$x: ${precision(index)._1 / precision(index)._2 * 100}%.2f%%, Intersect Count: ${precision(index)._1}, Recall Count:${precision(index)._2}")
                println(f"Recall@$x: ${recall(index)._1 / recall(index)._2 * 100}%.2f%%, Intersect Count: ${recall(index)._1}, Click Count:${recall(index)._2}")
                println(f"NDCG@$x: ${ndcg_k(index) / count * 100}%.2f%%")
                println
        }
    }

}