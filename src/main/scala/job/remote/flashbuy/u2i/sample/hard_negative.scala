package job.remote.flashbuy.u2i.sample

import com.github.jelmerk.knn.scalalike.bruteforce.BruteForceIndex
import job.remote.flashbuy.u2i.{UserInfo, SkuInfo}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.FileOperations
import utils.SparkJobs.RemoteSparkJob

import scala.reflect.ClassTag
import com.github.jelmerk.knn.scalalike.floatInnerProduct


object hard_negative extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.dt
        val timestamp = params.timestamp
        val src_table_name = params.src_table_name
        val dst_table_name = params.dst_table_name
        val threshold = params.threshold
        val window = params.window

        val user_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$timestamp/user_embedding/$dt"
        val sku_path = s"viewfs://hadoop-meituan/user/hadoop-hmart-waimaiad/yangyufeng04/bigmodel/multirecall/$timestamp/sku_embedding/$dt"
        if (!FileOperations.waitUntilFileExist(hdfs, user_path)) { sc.stop(); return }
        if (!FileOperations.waitUntilFileExist(hdfs, sku_path)) { sc.stop(); return }
        val user_emb = read_raw[String](user_path)
        val sku_emb = read_raw[String](sku_path)
        val dim = user_emb.take(1)(0)._2.length


        val pos = spark.sql(s"""
             select event_type, request_id, uuid, user_id, sku_id, spu_id, poi_id
               from $src_table_name
              where dt=$dt
               and sku_id is not null
               and event_type = 'click'
        """).as[ModelSample].rdd

        val poi_uuid = pos
          .map{x => (x.uuid, x.poi_id)}.distinct
          .join(user_emb)
          .map{ case (uuid, (poi_id, emb)) => (poi_id, UserInfo(uuid, emb)) }
          .groupByKey

        val poi_sku = spark.sql(
            s"""
               |select h.sku_id,
               |       coalesce(product_spu_id, 0) as product_spu_id,
               |       poi_id
               |  from mart_waimaiad.recsys_linshou_pt_poi_skus_high_quality h
               |  join (
               |       select sku_id, product_spu_id
               |         from mart_waimaiad.recsys_linshou_pt_poi_skus
               |        where dt='$dt'
               | ) t on h.sku_id=t.sku_id
               | where dt='$dt'
               |""".stripMargin).rdd.map { row =>
            val sku_id = String.valueOf(row.getAs[Long](0))
            val spu_id = String.valueOf(row.getAs[Long](1))
            val poi_id = row.getAs[Long](2)
            (sku_id, (poi_id, spu_id))
        }.join(sku_emb)
          .map { case (sku_id, ((poi_id, spu_id), emb)) => (poi_id, SkuInfo(sku_id + "," + spu_id, emb)) }
          .groupByKey

        val neg_hard = poi_sku.join(poi_uuid).flatMap {
            case (poi_id, (skus, users)) =>
                val index = BruteForceIndex[String, Array[Float], SkuInfo, Float](dim, floatInnerProduct)
                index.addAll(skus)
                users.par.map { user =>
                    val skuArr = index.findNearest(user.vector, threshold + window).map(re => re.item().id).toArray.slice(threshold, threshold + window) // 找到最近的n个然后
                    ((user.id, poi_id), skuArr)
                }.toList
        }
        val df = pos.map(x => ((x.uuid, x.poi_id), x)).join(neg_hard).values.flatMap{
            case (pos, skuArr) =>
                skuArr.map{x =>
                    require(x.length == 2)
                    val sku_id = x.split(",")(0).toLong
                    val spu_id = x.split(",")(1).toLong
                    ModelSample("view", pos.request_id, pos.uuid, pos.user_id, sku_id, Some(spu_id), pos.poi_id)
                }
        }.map {
            case ModelSample(event_type, request_id, uuid, user_id, sku_id, spu_id, poi_id) =>
                (event_type, request_id, uuid, user_id, sku_id, spu_id, poi_id)
        }.toDF("event_type", "request_id", "uuid", "user_id", "sku_id", "spu_id", "poi_id")
        FileOperations.saveAsTable(spark, df, dst_table_name, Map("dt" -> s"$dt", "threshold"-> threshold))
    }

    def read_raw[T: ClassTag](path: String)(implicit sc: SparkContext): RDD[(T, Array[Float])] = {
        sc.textFile(path).map { row =>
            val id = row.split(",")(0).asInstanceOf[T]
            val emb = row.split(",").drop(1).map(_.toFloat)
            (id, emb)
        }
    }
}
