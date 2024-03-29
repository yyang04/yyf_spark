package waimai.utils.AF

import scala.collection.mutable

class ArrayUnionFind (S: Set[Int])
{
    var group: mutable.Map[Int, Int] = mutable.Map() ++= S.map(x => (x, x))
    var size: mutable.Map[Int, Int] = mutable.Map() ++= S.map(x => (x, 1))
    var items: mutable.Map[Int, mutable.ListBuffer[Int]] = mutable.Map() ++= S.map(x => (x, mutable.ListBuffer(x)))

    def find(s: Int): Int = group(s)

    def union(a: Int, b:Int): Unit = {
        assert((items contains a) && (items contains b))
        val (c, d) = if (size(a) > size(b)) (b, a) else (a, b)
        for (s <- items(c)) {
            group(s) = d
            items(d).append(s)
        }
        size(d) += size(c)
        size -= c
        items -= c
    }

    def get_items: List[Int] = items.keys.toList
    def get_partitions: List[mutable.ListBuffer[Int]] = items.values.toList

    def drop_lower_bounds(lowerBounds: Int): ArrayUnionFind = {
        items = items.filter(x => x._2.length >= lowerBounds)
        size = size.filter(x => x._2 >= lowerBounds)
        this
    }
}

