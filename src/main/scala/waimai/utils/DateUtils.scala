package waimai.utils

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.ArrayBuffer


object DateUtils{
    // 时间戳如果是秒是16亿
    // 时间戳如果是毫秒则是1万6千亿
    val dtFormat = new SimpleDateFormat("yyyyMMdd")

    def dt2TimeStamp(dt: String): Long = dtFormat.parse(dt).getTime   // dt转ts

    /**
     * 获取N天前的时间字符串
     *
     * @param num 代表N
     * @return "yyyyMMdd"
     */
    def getNDaysAgo(num: Int): String = {
        val c = Calendar.getInstance()
        c.add(Calendar.DATE, -num)
        dtFormat.format(c.getTime)
    }

    def getNDaysAgoFrom(dt: String, num: Int): String = {
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

    // 包括首尾
    def getDtRange(begin: String, end: String): Iterable[String] = {
        val beginDt = dtFormat.parse(begin)
        val c = Calendar.getInstance()
        c.setTime(beginDt)

        val endDt = dtFormat.parse(end)
        val c1 = Calendar.getInstance()
        c1.setTime(endDt)

        val diff = (c1.getTimeInMillis - c.getTimeInMillis) / 1000 / 3600 / 24
        val rs = new ArrayBuffer[String]()
        rs.append(begin)
        for (i <- 0L until diff) {
            c.add(Calendar.DATE, 1)
            rs.append(dtFormat.format(c.getTime))
        }
        rs
    }

    // 获取当前时间戳s
    def getTs: Int = {
        val cal = Calendar.getInstance()
        (cal.getTimeInMillis / 1000).toInt
    }

    // 获取当前星期几
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
        ((cal.getTimeInMillis - System.currentTimeMillis()) / 1000).toInt
    }

    // 今天的24点的时间戳 s
    def getTsForDawn: Int = {
        val cal = Calendar.getInstance()
        cal.add(Calendar.DAY_OF_YEAR, 1)
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        (cal.getTimeInMillis / 1000).toInt - 1
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

    // 下个星期的时间戳 s
    def getTsForNextWeek: Int = {
        val cal = Calendar.getInstance()
        cal.add(Calendar.DAY_OF_YEAR, 7)
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        (cal.getTimeInMillis / 1000).toInt - 1
    }

    // 下个小时的时间戳 s
    def getTsForNextHour(hour: Int): Int = {
        val cal = Calendar.getInstance()
        cal.add(Calendar.HOUR_OF_DAY, hour)
        (cal.getTimeInMillis / 1000).toInt - 1
    }

    // 今天0点时间戳 s
    def getTsForBeginOfDay: Int = {
        val cal: Calendar = Calendar.getInstance()
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        (cal.getTimeInMillis / 1000).toInt - 1
    }

    // 时间戳ms, 转换成小时
    def getHourFromTs(ts: Long): Int = {
        val c = Calendar.getInstance()
        c.setTimeInMillis(ts)
        c.get(Calendar.HOUR_OF_DAY)
    }

    // 时间戳ms，转换成周末或者周中，周末为1，周中为0
    def getWeekDayFromTs(ts: Long): Int = {
        val c = Calendar.getInstance()
        c.setTimeInMillis(ts)
        val ret = c.get(Calendar.DAY_OF_WEEK)
        if (ret < 6 && ret > 0) 0 else 1
    }


    def main(args: Array[String]): Unit = {
        val c1 = Calendar.getInstance()
        println(c1.getTime.getTime)
        println(c1.getTimeInMillis)
//        getDtRange("20210101", "20210102").foreach(println(_)) // 20210101, 20210102
//        println(getHourFromTs(1594787450270L))
//        println(getWeekDayFromTs(1677734274397L))
    }
}

