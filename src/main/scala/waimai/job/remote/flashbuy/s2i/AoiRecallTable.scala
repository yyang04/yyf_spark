package waimai.job.remote.flashbuy.s2i

import waimai.utils.SparkJobs.RemoteSparkJob
import waimai.utils.DateUtils.{getFoodTime, getHourFromTs, getNDaysAgoFrom, getWeekDayFromTs}
import waimai.utils.FileOp

object AoiRecallTable extends RemoteSparkJob {
    override def run(): Unit = {
        val dt = params.dt
        val version = params.version

        val pt_poi_aoi = spark.sql(
            s"""
               | select a.poi_id,
               |        sku_id,
               |        event_timestamp,
               |        sku_second_category_id,
               |        aoi_type_id
               |  from mart_waimaiad.flashbuy_channelpage_click_item_aoi a
               |  join
               |    (select distinct poi_id
               |       from mart_waimaiad.pt_flashbuy_expose_poi_daily_v1
               |      where dt between ${getNDaysAgoFrom(dt, 10)} and $dt
               |   ) b on a.poi_id=b.poi_id
               |  where dt between 20221201 and $dt
               |""".stripMargin).rdd.map{ row =>
            val poi_id = row.getAs[String](0)
            val sku_id = row.getAs[String](1).toLong
            val event_timestamp = row.getAs[Long](2)
            val cate2 = row.getAs[Long](3)
            val aoi_type_id = row.getAs[Int](4).toString
            val week = getWeekDayFromTs(event_timestamp).toString
            val foodTime = getFoodTime(getHourFromTs(event_timestamp)).toString
            Sample(poi_id, aoi_type_id, s"${week}_$foodTime", sku_id, cate2)
        }.cache

        val topK = pt_poi_aoi.groupBy{ x => (x.poi_id, x.cate2) }.map{
            case (key, iter) =>
                var skuScore = iter.groupBy(_.sku_id).mapValues(_.size).filter(_._2 >= 2).toArray.sortBy(-_._2).take(5).map{ x=> SkuScore(x._1, x._2.toDouble)}
                val total = skuScore.map(_.score).sum
                skuScore = skuScore.map{ case SkuScore(skuId, skuScore) => SkuScore(skuId, skuScore / total)}
                (key, skuScore)
        }.filter(_._2.length != 0).map{ case((poi_id, cate2), skuScore) => (poi_id, (cate2, skuScore)) }


        val aoi_time_category = spark.sql(
            s"""
               |select aoi_type_id,
               |       event_timestamp,
               |       sku_second_category_id
               |   from mart_waimaiad.flashbuy_channelpage_click_item_aoi
               |  where dt between 20221201 and $dt
               |  group by 1,2,3
               |""".stripMargin).rdd.map{ row =>
            val aoi_type_id = row.getAs[Long](0)
            val event_timestamp = row.getAs[Long](1)
            val cate2 = row.getAs[Long](2)
            val week = getWeekDayFromTs(event_timestamp).toString
            val foodTime = getFoodTime(getHourFromTs(event_timestamp)).toString
            val time = s"${week}_$foodTime"
            ((aoi_type_id, time), cate2)
        }.groupByKey.mapValues{ iter =>
            iter.groupBy(identity).mapValues{_.size}.toArray.sortBy(-_._2).take(4).map(_._1)}
          .collect.toMap

        val df = topK.groupByKey.flatMap{
            case (poi_id, iter) =>
                val cateMap = iter.toMap // cate skuScore //aoi_type + time, cate
                aoi_time_category.map{
                    case (k, v) =>
                        val ret = v.flatMap{ x =>  cateMap.getOrElse(x, Array()) }.map{
                            case SkuScore(skuId, weight)  => f"$skuId:$weight%.4f"
                        }.mkString(",")
                        (s"${poi_id}_${k._1}_${k._2}", ret)
                }.toList
        }.toDF("key", "value")
        FileOp.saveAsTable(spark, df, "recsys_linshou_multi_recall_results_v2" + version,
            Map("method" -> "eges", "date" -> dt, "branch" -> "u2i"))




//        val pt_poi_aoi = spark.sql(
//            s"""
//               | select a.poi_id,
//               |        sku_id,
//               |        event_timestamp,
//               |        sku_second_category_id,
//               |        aoi_type_id
//               |  from mart_waimaiad.flashbuy_channelpage_click_item_aoi a
//               |  join
//               |    (select distinct poi_id
//               |       from mart_waimaiad.pt_flashbuy_expose_poi_daily_v1
//               |      where dt between 20230220z and 20230301
//               |   ) b on a.poi_id=b.poi_id
//               |  where dt between 20230131 and 20230301
//               |""".stripMargin).rdd.map { row =>
//            val poi_id = row.getAs[String](0)
//            val sku_id = row.getAs[String](1).toLong
//            val event_timestamp = row.getAs[Long](2)
//            val cate2 = row.getAs[Long](3)
//            val aoi_type_id = row.getAs[Long](4).toString
//            val week = getWeekDayFromTs(event_timestamp).toString
//            val foodTime = getFoodTime(getHourFromTs(event_timestamp)).toString
//            Sample(s"${poi_id}_$aoi_type_id", s"${week}_$foodTime", sku_id, cate2)
//        }
//
//
//
//
//
//
//
//
//
//        val topKCate = pt_poi_aoi.groupBy(_.poi_aoi).mapValues{ iter =>
//            iter.groupBy(_.sku_second_category_id).mapValues(_.size).toList.sortBy(-_._2).map(_._1).take(8)
//        }
//
//        val topKCateTime = pt_poi_aoi.groupBy(x => (x.poi_aoi, x.time)).map { case(key ,iter) =>
//            (key._1, (key._2, iter.groupBy(_.sku_second_category_id).mapValues(_.size).toList.sortBy(-_._2).map(_._1).take(5)))
//        }.join(topKCate).flatMap{
//            case (key, (v1, v2)) =>
//                (v1._2 ++ v2).distinct.take(5).map(x=> ((key, v1._1, x), 1))
//        }
//
//        val topSku = pt_poi_aoi.groupBy(x => (x.poi_aoi, x.time, x.sku_second_category_id)).map{ case(key, iter) =>
//            (key, iter.groupBy(_.sku_id).mapValues(_.size).toList.sortBy(_._2).map(_._1).take(3))
//        }.join(topKCateTime).flatMap{
//            case((p, t, c), (skuList, 1)) =>
//                skuList.map{ x => ((p, t), (c, x))}
//        }.groupByKey.map{
//            case (key, iter) =>
//                iter.toList
//        }
    }

}
