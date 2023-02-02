package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait S3Connect {

  private var host: String = "s3plus.vip.sankuai.com"
  private var secretKey: String = "9d0ecb5da3f84f57b8ff26aa8414dcf6"
  private var accessKey: String = "d4f23cbe570e4268a871291726a2ff5e"

  @transient
  var s3Handler: S3Handler = _

  def initConnect(sc: SparkContext): Boolean = {
    initAmazonS3ConnOnSparkContext(sc, accessKey, secretKey, host)
    s3Handler = S3Handler.getInstance(accessKey, secretKey)
    true
  }

  def initAmazonS3ConnOnSparkContext(sc: SparkContext, accessKey: String, secretKey: String, host: String): Boolean = {
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3a.access.key", accessKey)
    hadoopConf.set("fs.s3a.secret.key", secretKey)
    hadoopConf.set("fs.s3a.endpoint", host)
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3a.path.style.access", "true")
    true
  }


  def getS3Url(bucket: String, objName: String): String = {
    s"s3a://$bucket/$objName/"
  }

  def readTextFile(sc: SparkContext, bucket: String, objName: String): RDD[String] = {
    sc.textFile(getS3Url(bucket, objName))
  }
}
