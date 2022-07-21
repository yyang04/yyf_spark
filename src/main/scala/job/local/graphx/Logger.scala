package job.local.graphx

import org.apache.log4j.Logger
import org.apache.log4j.Level


trait Logger {
    private val loggerName = this.getClass.getName
    private lazy val logger = Logger.getLogger(loggerName)
    def info(msg: => String): Unit = logger.info(msg)
    def warn(msg: => String): Unit = logger.warn(msg)
    def debug(msg: => String): Unit = logger.debug(msg)
    def setLevel(level: Level): Unit = logger.setLevel(level)
    setLevel(Level.INFO)
}
