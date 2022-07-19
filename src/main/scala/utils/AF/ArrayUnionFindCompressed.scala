package utils.AF

import scala.collection.mutable

class ArrayUnionFindCompressed(S: Set[Int]) {
    var group: mutable.Map[Int, Int] = mutable.Map() ++= S.map(x => (x, x))
    var size: mutable.Map[Int, Int] = mutable.Map() ++= S.map(x => (x, 1))

    def find(s: Int): Int = {
        val res = if (s == group(s)) {
            s
        } else {
            group(s) = find(group(s))
            group(s)
        }
        res
    }

    def union(a: Int, b: Int): Unit = {
        val (c, d) = if (size(a) > size(b)) (b, a) else (a, b)
        group(c) = d
        size(d) += size(c)
        size -= c
    }
}