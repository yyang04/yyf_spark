package utils

import java.sql.Timestamp
import java.text.SimpleDateFormat

object TimeOperations {
    def getTimestamp(x: String) : Long = {
        val format = new SimpleDateFormat("yyyyMMdd")
        val d = format.parse(x)
        d.getTime
    }
}
