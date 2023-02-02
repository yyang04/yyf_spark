package utils

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client, S3ClientOptions}
import com.amazonaws.{ClientConfiguration, Protocol}
import java.io.{File, FileInputStream}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class S3Handler(s3Client: AmazonS3) {
    def getS3Client: AmazonS3 = {
        s3Client
    }

    def uploadMultiplePartObject(localFilePath: String, bucketName: String, objectName: String): Unit = {
        val partSize = 1024 * 1024 * 100L
        val partETags = ListBuffer[PartETag]()
        val initRequest = new InitiateMultipartUploadRequest(bucketName, objectName)
        val initResponse = s3Client.initiateMultipartUpload(initRequest)
        val file: File = new File(localFilePath)
        val contentLength: Long = file.length
        println(s"PartSize:${partSize / 1024 / 1024}MB")
        println(s"contentLength:${contentLength / 1024 / 1024}MB")
        if (contentLength < 1024 * 1024 * 1L) {
            putObjectFile(localFilePath, bucketName, objectName)
            return
        }
        Try {
            var filePosition = 0L
            var i = 1
            while (filePosition < contentLength) {
                val tmpPartSize: Long = Math.min(partSize, contentLength - filePosition)
                val tmpPartSizeStr = if (tmpPartSize > 1024 * 1024) s"${tmpPartSize / 1024 / 1024} mb" else s"${tmpPartSize / 1024} kb"
                println(s"TmpPartSize:$tmpPartSizeStr")
                val uploadRequest = (new UploadPartRequest)
                  .withBucketName(bucketName)
                  .withKey(objectName)
                  .withUploadId(initResponse.getUploadId)
                  .withPartNumber(i)
                  .withFileOffset(filePosition)
                  .withFile(file)
                  .withPartSize(tmpPartSize)
                partETags += s3Client.uploadPart(uploadRequest).getPartETag
                filePosition += tmpPartSize
                i += 1
            }
            val compRequest = new CompleteMultipartUploadRequest(bucketName, objectName, initResponse.getUploadId, partETags)
            s3Client.completeMultipartUpload(compRequest)
        } match {
            case Failure(exception) =>
                println(s"S3 upload failed,${exception.printStackTrace()}")
                s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, objectName, initResponse.getUploadId))
                //        throw (s"S3(bucket:$bucketName,object:$objectName) upload failed...source file: ${localFilePath} ")

            case Success(_) =>
        }
    }

    def putObjectFile(uploadFileName: String, bucketName: String, objectName: String): Unit = {
        val inputStream = new FileInputStream(uploadFileName)
        s3Client.putObject(bucketName, objectName, inputStream, null)
    }

    def getObjectToLocalFile(bucketName: String, objectName: String, localFilePath: String, localFileName: String): String = {
        val localFile = localFilePath + File.separator + localFileName
        s3Client.getObject(new GetObjectRequest(bucketName, objectName), new File(localFile))
        localFile
    }

    def deleteS3Directory(bucket: String, dir: String): Unit = {
        val keys = s3Client.listObjects(bucket, dir + "/")
          .getObjectSummaries
          .map(e => new DeleteObjectsRequest.KeyVersion(e.getKey))
        if (keys.nonEmpty) {
            val toDelete = keys.map(e => e.getKey).mkString(",")
            println(s"S3 Objects to delete：$toDelete")
            val req = new DeleteObjectsRequest(bucket).withKeys(keys)
            val resp = s3Client.deleteObjects(req)
            val deleted = resp.getDeletedObjects.map(e => e.getKey).mkString(",")
            println(s"S3 Objects deleted：$deleted")
        }
    }
}

object S3Handler extends Serializable {
    private val host = "s3plus.vip.sankuai.com"

    private val accessKey = "d4f23cbe570e4268a871291726a2ff5e"

    private val secretKey = "9d0ecb5da3f84f57b8ff26aa8414dcf6"

    lazy val s3Handler: S3Handler = getInstance()

    def getInstance(accessKey: String = "d4f23cbe570e4268a871291726a2ff5e", secretKey: String = "9d0ecb5da3f84f57b8ff26aa8414dcf6"): S3Handler = {
        new S3Handler(createAmazonS3Client(accessKey, secretKey))
    }


    def createAmazonS3Client(accessKey: String, secretKey: String): AmazonS3 = {
        val credentials = new BasicAWSCredentials(accessKey, secretKey)
        val configuration = new ClientConfiguration
        // 默认协议为HTTPS。将这个值设置为Protocol.HTTP，则使用的是HTTP协议
        configuration.setProtocol(Protocol.HTTPS)
        //生成云存储api client
        val mssClient = new AmazonS3Client(credentials, configuration)
        //配置云存储服务地址 https://km.sankuai.com/page/194870634
        mssClient.setEndpoint(host)
        //设置客户端生成的http请求hos格式，目前只支持path type的格式，不支持bucket域名的格式
        val s3ClientOptions = new S3ClientOptions
        s3ClientOptions.setPathStyleAccess(true)
        mssClient.setS3ClientOptions(s3ClientOptions)
        mssClient
    }


    def uploadMultiplePartObject(localFilePath: String, bucketName: String, objectName: String): Unit = {
        //s3上传data文件和md5文件
        s3Handler.uploadMultiplePartObject(localFilePath, bucketName, objectName)
    }


    def putObjectFile(uploadFileName: String, bucketName: String, objectName: String): Unit = {
        s3Handler.putObjectFile(uploadFileName, bucketName, objectName)
    }
}