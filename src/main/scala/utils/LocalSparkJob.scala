package utils

import org.apache.log4j.{Level, Logger}

abstract class LocalSparkJob{
    Logger.getLogger("org").setLevel(Level.ERROR)

}
