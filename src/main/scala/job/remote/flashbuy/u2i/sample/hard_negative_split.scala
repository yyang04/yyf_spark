package job.remote.flashbuy.u2i.sample

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import utils.FileOperations
import utils.SparkJobs.RemoteSparkJob

import scala.collection.mutable
import scala.reflect.ClassTag

object hard_negative_split extends RemoteSparkJob{
    override def run(): Unit = {
        val dt = params.dt
        val timestamp = params.timestamp
        val user_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$timestamp/user_embedding/$dt"
        val sku_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$timestamp/sku_embedding/$dt"
        if (!FileOperations.waitUntilFileExist(hdfs, user_path)) { sc.stop(); return }
        if (!FileOperations.waitUntilFileExist(hdfs, sku_path)) { sc.stop(); return }
        val user_emb = read_raw[String](user_path)
        val sku_emb = read_raw[String](sku_path)
        writeFile(user_emb, "user_embedding", timestamp)
        writeFile(sku_emb, "sku_embedding", timestamp)
    }

    def read_raw[T: ClassTag](path: String)(implicit sc: SparkContext): RDD[(String, (T, Array[Float]))] = {
        sc.textFile(path).map { row =>
            val dt = row.split(",")(0)
            val id = row.split(",")(1).asInstanceOf[T]
            val emb = row.split(",").drop(2).map(_.toFloat)
            (dt, (id, emb))
        }
    }

    def writeFile(data: RDD[(String, (String, Array[Float]))], mode: String, timestamp: String)(implicit hdfs: FileSystem): Unit = {
        data.groupByKey.foreach {
            case (dt, iter) =>
                val path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$timestamp/$mode/$dt"
                val iterator = iter.toIterator
                val data = mutable.ListBuffer[String]()
                var index = 0
                while (iterator.hasNext) {
                    val element = iterator.next
                    val result = s"${element._1},${element._2.mkString(",")}"
                    data.append(result)
                    if (data.size > 10000) {
                        FileOperations.saveTextFile(hdfs, data, path + s"/part-$index")
                        data.clear
                        index += 1
                    }
                }
                if (data.nonEmpty) {
                    FileOperations.saveTextFile(hdfs, data, path)
                }
        }

    }
}