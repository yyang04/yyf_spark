package waimai.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}
import scala.collection.mutable.ArrayBuffer


object DateUtils{
    // 20w key => 2000000 * 73

    val dtFormat = new SimpleDateFormat("yyyyMMdd")         // 年月日
//    val fpFormat = new SimpleDateFormat("yyyyMMddHH")       // 年月日小时
//    val dvFormat = new SimpleDateFormat("yyyyMMddHHmmss")   // 年月日小时分钟秒
//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")   // 年月日小时分钟秒
    val stdTs = 1638172000L

//    def timeStamp2Date(ts: Long): String = sdf.format(new Date(ts))
    def dt2TimeStamp(dt: String): Long = dtFormat.parse(dt).getTime   // dt转ts

    /**
     * 获取N天前的时间字符串
     * @param num  代表N
     * @return
     */
    def getNDaysAgo(num: Int): String = {
        val c = Calendar.getInstance()
        c.add(Calendar.DATE, -num)
        dtFormat.format(c.getTime)
    }


    def getNDaysAgoFrom(dt: String, num: Int):String={
        val c = Calendar.getInstance()
        try {
            val date = dtFormat.parse(dt)
            c.setTime(date)
        } catch {
            case e: Exception => printf(s"parse dt error, dt = $dt")
        }
        c.add(Calendar.DATE, -num)
        dtFormat.format(c.getTime)
    }

    def genDtRange(begin: String, end: String): Iterable[String] = {
        val beginDt = dtFormat.parse(begin)
        val c = Calendar.getInstance()
        c.setTime(beginDt)

        val endDt = dtFormat.parse(end)
        val c1 = Calendar.getInstance()
        c1.setTime(endDt)

        val diff = (c1.getTime.getTime - c.getTime.getTime) / 3600000 / 24
        val rs = new ArrayBuffer[String]()
        rs.append(begin)
        for (i <- 0L until diff) {
            c.add(Calendar.DATE, 1)
            rs.append(dtFormat.format(c.getTime))
        }
        rs
    }

    def getTs: Int = {
        val cal = Calendar.getInstance()
        (cal.getTimeInMillis / 1000).toInt
    }


    def getWeekDay(dt: String): Int = {
        val c = Calendar.getInstance()
        c.setTime(dtFormat.parse(dt))
        c.get(Calendar.DAY_OF_WEEK)
    }


    /**
     * 根据hour得到餐段，早餐-1，午餐-2，下午茶-3，晚餐-4，夜宵-5
     * @param hour
     */
    def getFoodTime(hour: Int): Int = {
        var period = 0
        if (hour >=5 && hour<=9) {
            period = 1
        } else if (hour>=10 && hour <= 13) {
            period = 2
        } else if (hour>=14 && hour <= 16) {
            period = 3
        } else if (hour>=17 && hour <= 21) {
            period = 4
        } else {
            period = 5
        }
        period
    }

    /**
     * 当前时间距离24点有多少秒
     */
    def getSecondsBeforeDawn: Int = {
        val cal = Calendar.getInstance()
        cal.add(Calendar.DAY_OF_YEAR, 1)
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.MILLISECOND, 0)
        ((cal.getTimeInMillis() - System.currentTimeMillis()) / 1000).toInt
    }


    /**
     * 当前时间距离下一小时有多少秒
     */
    def getSecondsBeforeNextHour: Int = {
        val cal = Calendar.getInstance()
        cal.add(Calendar.HOUR_OF_DAY, 1)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.MILLISECOND, 0)
        ((cal.getTimeInMillis - System.currentTimeMillis()) / 1000).toInt
    }


    def getTsForDawn: Int = {
        val cal = Calendar.getInstance()
        cal.add(Calendar.DAY_OF_YEAR, 1)
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        (cal.getTimeInMillis / 1000).toInt - 1
    }


    def getTsForNextHour(hour: Int): Int = {
        val cal = Calendar.getInstance()
        cal.add(Calendar.HOUR_OF_DAY, hour)
        (cal.getTimeInMillis / 1000).toInt - 1
    }

    def getTsForBeginOfDay: Int = {
        val cal: Calendar = Calendar.getInstance()
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        (cal.getTimeInMillis / 1000).toInt - 1
    }
}

