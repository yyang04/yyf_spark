package utils
import org.apache.spark.rdd.RDD
import utils.AF.{ArrayUnionFind, ArrayUnionFindCompressed}

import scala.collection.mutable
import scala.util.control.Breaks
import scala.util.Random

object MST {
    // Kruskal Local Minimum Spanning Tree
    def MSTKLocal(ed: RDD[(Int, Int, Double)]): Array[(Int, Int, Double)] = {
        var E = ed.collect()
        val mst = new mutable.ListBuffer[(Int, Int, Double)]
        E = E.sortBy(x => x._3)
        val V = E.flatMap(x => List(x._1, x._2)).distinct.toSet
        val UF = new ArrayUnionFind(V)
        E.foreach(e => {
            val u_group = UF.find(e._1)
            val v_group = UF.find(e._2)
            if (u_group != v_group) {
                mst.append(e)
                UF.union(u_group, v_group)
            }
        })
        mst.toArray
    }

    // Distributed Minimum Spanning Tree Realized by Google
    def MSTK(ed: RDD[(Int, Int, Float)]): Array[(Int, Int, Float)] = {
        var edges = ed
        var m = edges.count()
        val vertices = edges.flatMap(x => List(x._1, x._2)).distinct()
        val n = vertices.count()
        val eps = 0.01
        var c = math.ceil(math.log(m)) / math.ceil(math.log(n)) - 1
        while (c > eps) {
            val k = math.floor(math.pow(n, (c - eps) / 2)).toInt
            c = math.ceil(math.log(m)) / math.ceil(math.log(n)) - 1
            val keyedEdges = edges.map(x => (x._1, x))
            val half_partitioning = keyedEdges.groupByKey().flatMap(x => {
                val edges = x._2
                val out = new mutable.ListBuffer[(Int, (Int, (Int, Int, Float)))]
                val partitionKey = Random.nextInt(k)
                for (e <- edges) {
                    out.append((e._2, (partitionKey, e)))
                }
                out
            })
            val full_partitioning = half_partitioning.groupByKey().flatMap(x => {
                val edges = x._2
                val out = new mutable.ListBuffer[((Int, Int), (Int, Int, Float))]
                val partitionKey = Random.nextInt(k)
                for (e <- edges) {
                    val firstPartition = e._1
                    val edge = e._2
                    out.append(((firstPartition, partitionKey), edge))
                }
                out
            })
            edges = full_partitioning.groupByKey().flatMap(x => {
                val arr = x._2
                val mst = new mutable.ListBuffer[(Int, Int, Float)]
                var E = arr.toList
                E = E.sortBy(x => x._3)
                val V = E.flatMap(x => List(x._1, x._2)).distinct.toSet
                val UF = new ArrayUnionFindCompressed(V)
                E.foreach(e => {
                    val u_group = UF.find(e._1)
                    val v_group = UF.find(e._2)
                    if (u_group != v_group) {
                        mst.append(e)
                        UF.union(u_group, v_group)
                    }
                })
                mst
            })
            m = edges.count()
        }
        edges.collect()
    }

    // Local Minimum Spanning Tree for clustering
    def MSTBClustering(ed: Array[(Int, Int, Float)], upperBounds: Int): ArrayUnionFind = {
        var edges = ed.toSet
        val mst = new mutable.ListBuffer[mutable.ListBuffer[(Int, Int, Float)]]()
        val V = edges.toList.flatMap(x => List(x._1, x._2)).distinct.toSet
        val UF = new ArrayUnionFind(V)

        val max_iterations = math.ceil(math.log(upperBounds) / math.log(2)).toInt

        Breaks.breakable {
            for (_ <- 0 until max_iterations) {
                val mst_step = mutable.ListBuffer[(Int, Int, Float)]()
                val pre = UF.get_items.length
                val min_edges = mutable.Map[Int, (Int, Int, Float)]()
                for (e <- edges) {
                    if (!(min_edges contains e._1) || (min_edges(e._1)._3 > e._3)) {
                        min_edges += e._1 -> e
                    }
                    if (!(min_edges contains e._2) || (min_edges(e._2)._3 > e._3)) {
                        min_edges += e._2 -> (e._2, e._1, e._3)
                    }
                }
                for ((u, v, w) <- min_edges.values) {
                    val u_parent = UF.find(u)
                    val v_parent = UF.find(v)
                    val u_size = UF.size(u_parent)
                    val v_size = UF.size(v_parent)

                    if (u_parent != v_parent && u_size < upperBounds && v_size < upperBounds) {
                        UF.union(u_parent, v_parent)
                        mst_step.append((u, v, w))
                    }
                }
                if (pre == UF.get_items.length) Breaks.break()
                edges = edges -- mst_step.toSet
                mst.append(mst_step)
            }
        }
        UF
    }

}
