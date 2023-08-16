package waimai.utils

object MapOp {
    def mergeMap(x: Map[String, Int], y: Map[String, Int]): Map[String, Int] = {
        x.foldLeft(y)(
            (mergedMap, kv) => mergedMap + (kv._1 -> (mergedMap.getOrElse(kv._1, 0) + kv._2))
        )
    }
}
