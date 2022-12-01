package job.remote.flashbuy

import utils.JSONUtils.jsonPv
import utils.SparkJobs.RemoteSparkJob

object RecallCoverRate extends RemoteSparkJob{

    def mergedMap(x: Map[String, Int], y: Map[String, Int]): Map[String, Int] = {
        x.foldLeft(y)(
            (mergedMap, kv) => mergedMap + (kv._1 -> (mergedMap.getOrElse(kv._1, 0) + kv._2))
        )
    }

    def handleMap(x: Map[String, Int]): Array[(String, String)] = {
        val total = x("total")
        val result = x.toArray.sortBy(_._2).reverse.map { case (k, v) => (k, f"${v.toDouble / total * 100}%.2f%%") }
        Array(("total", f"$total")) ++ result
    }

    override def run(): Unit = {
        val dt = params.dt
        val poi = spark.sql(
            s"""
               |select dt,
               |       wm_poi_id
               |  from mart_waimai.aggr_poi_info_dd
               | where dt='$dt' and primary_first_tag_id in 13000000
               |""".stripMargin).rdd.map{ row=>
            val wm_poi_id = row.getAs[Long](0)
            wm_poi_id.toString
        }.collect()

        val broad_poi = sc.broadcast(poi)

        val pv = spark.sql(
            s"""select pvid,
               |       case when exp_id like '%184594%' and exp_id like '%185967%' then 'base'
               |            when exp_id like '%184594%' and exp_id like '%185966%' then 'uuid'
               |            when exp_id like '%184462%' and exp_id like '%185967%' then 'cid'
               |            when exp_id like '%184462%' and exp_id like '%185966%' then 'cid_uuid'
               |            when exp_id like '%144570%' and exp_id like '%185967%' then 'cid_sku'
               |            when exp_id like '%144570%' and exp_id like '%185966%' then 'cid_sku_uuid'
               |            else 'other' end as exp_id,
               |        recallresults
               |   from (
               |      select pvid,
               |             get_json_object(expids, '$$.frame_exp_list') exp_id,
               |             recallresults
               |        from log.adt_multirecall_pv
               |       where dt='$dt' and scenetype='2'
               |""".stripMargin).rdd.mapPartitions{ iter =>
                 val poi = broad_poi.value
                 iter.map{ row =>
                     val pvId = row.getAs[String](0)
                     val exp_id = row.getAs[String](1)
                     val recallResults = row.getAs[String](2)
                     val parseResults = jsonPv(recallResults)
                     val res = parseResults.filterKeys(poi contains _).values.map{
                         x => x.take(4)
                           .flatMap{x => x._2.split(",")}
                           .groupBy(identity).mapValues(_.length) ++ Map("total" -> 4)
                     }.reduce((x,y) => mergedMap(x,y))
                     (exp_id, res)
                 }
        }.reduceByKey((x,y) => mergedMap(x,y)).collect

        pv.foreach {
            case (exp_id, resultMap) =>
                println(s"""exp_id: $exp_id""")
                println(s"""sku粒度: ${handleMap(resultMap).mkString(",")}""")
                println()
        }
    }

}
