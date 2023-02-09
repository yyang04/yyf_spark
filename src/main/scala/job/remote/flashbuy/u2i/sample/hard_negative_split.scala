package job.remote.flashbuy.u2i.sample

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import utils.FileOperations
import utils.SparkJobs.RemoteSparkJob
import utils.TimeOperations.getDate

import scala.collection.mutable
import scala.reflect.ClassTag

class IDPartitioner(override val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = key match {
        case x: String => x.toInt
    }
}

object hard_negative_split extends RemoteSparkJob{
    override def run(): Unit = {
        val dt = params.dt            // 需要的时间
        val beginDt = params.beginDt  // 时间段
        val timestamp = params.timestamp

        val user_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$timestamp/user_embedding/$beginDt"
        val sku_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$timestamp/sku_embedding/$beginDt"
        if (!FileOperations.waitUntilFileExist(hdfs, user_path)) { sc.stop(); return }
        if (!FileOperations.waitUntilFileExist(hdfs, sku_path)) { sc.stop(); return }

        val user_emb = read_raw(user_path)
        val sku_emb = read_raw(sku_path)

        user_emb
          .filter(_._1 == dt)
          .saveAsTextFile(s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$timestamp/user_embedding/$dt")
        sku_emb
          .filter(_._1 == dt)
          .saveAsTextFile(s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$timestamp/sku_embedding/$dt")
    }



    def read_raw(path: String)(implicit sc: SparkContext): RDD[(String, String)] = {
        sc.textFile(path).map { row =>
            val dt = row.split(",")(0)
            val id_emb = row.split(",")(1).drop(1).mkString(",")
            (dt, id_emb)
        }
    }




    def writeFile(data: RDD[(String, (String, Array[Float]))], mode: String, timestamp: String)(implicit hdfs: FileSystem): Unit = {
//        val bHdfs = sc.broadcast(hdfs)
//        data.groupByKey.foreach {
//            case (dt, iter) =>
//                val hdfs = bHdfs.value
//                val path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$timestamp/$mode/$dt"
//                val iterator = iter.toIterator
//                val data = mutable.ListBuffer[String]()
//                var index = 0
//                while (iterator.hasNext) {
//                    val element = iterator.next
//                    val result = s"${element._1},${element._2.mkString(",")}"
//                    data.append(result)
//                    if (data.size > 10000) {
//                        FileOperations.saveTextFile(hdfs, data, path + s"/part-$index")
//                        data.clear
//                        index += 1
//                    }
//                }
//                if (data.nonEmpty) {
//                    FileOperations.saveTextFile(hdfs, data, path)
//                }
//        }
    }
}

