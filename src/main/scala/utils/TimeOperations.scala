package utils

import java.text.SimpleDateFormat
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}


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

    def main(args: Array[String]): Unit = {
        println(getDateDelta("20220908", 92))
    }

}
