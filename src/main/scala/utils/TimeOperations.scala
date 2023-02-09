package utils

import java.text.SimpleDateFormat
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days, Period}

import scala.annotation.tailrec


object TimeOperations {
    // example
    // val date_string = getDateRange("20211019", 38)
    // val date = getDateDelta("20211020", 1)

    def getTimestamp(x: String, pattern: String= "yyyyMMdd") : Long = {
        val format = new SimpleDateFormat(pattern)
        val d = format.parse(x)
        d.getTime
    }

    def getDateRange(beginDate: String, numDays: Int, formatPattern: String="YYYYMMdd"): List[String] ={
        val format = DateTimeFormat.forPattern(formatPattern)
        (0 to numDays).map(inc =>
            DateTime.parse(beginDate, format).plusDays(inc).toString(formatPattern)
        ).toList
    }

    def getDateDelta(date:String, delta:Int, formatPattern:String="YYYYMMdd"):String={
        val format = DateTimeFormat.forPattern(formatPattern)
        DateTime.parse(date, format).plusDays(delta).toString(formatPattern)
    }

    def getDate(beginDate: String, endDate: String, formatPattern: String= "YYYYMMdd"): List[String] = {
        val format = DateTimeFormat.forPattern(formatPattern)
        val from = DateTime.parse(beginDate, format)
        val to = DateTime.parse(endDate, format)
        val numberOfDays = Days.daysBetween(from, to).getDays

        getDateRange(beginDate, numberOfDays, formatPattern)
    }

    def main(args: Array[String]): Unit = {
        println(getDate("20220202", "20220212").mkString(","))
    }

}
