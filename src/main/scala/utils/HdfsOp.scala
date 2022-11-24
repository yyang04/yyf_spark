package utils

import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedWriter, OutputStreamWriter}
import scala.util.control.NonFatal

object HdfsOp {
    def openHdfsFile(path: String, fs: FileSystem): BufferedWriter = {
        val writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path))))
        writer
    }


    def write[T](writer: BufferedWriter, line: T): Unit = {
        try {
            writer.write(line.toString + "\n")
        } catch {
            case e: Exception => println("error")
        }
    }

    def closeHdfsFile(writer: BufferedWriter): Unit = {
        try {
            if (writer != null) {
                writer.close()
            }
        } catch {
            case NonFatal(e) => println("error")
        }
    }

    def copyToLocal(fs: FileSystem, src: String, dst: String): Unit = {
        val s = new Path(src)
        val d = new Path(dst)
        fs.copyToLocalFile(s, d)
    }
}