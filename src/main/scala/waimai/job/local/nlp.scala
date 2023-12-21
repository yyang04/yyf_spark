package waimai.job.local

import waimai.utils.SparkJobs.LocalSparkJob
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, VectorAssembler}
import org.apache.spark.sql.SparkSession
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline

object nlp extends LocalSparkJob {

//	def main(args: Array[String]): Unit = {
//
//		// 输入数据
//		val data = Seq("你好", "我是中国人", "欢迎来到中国")
//		val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("text")
//
//		// 创建DocumentAssembler
//		val documentAssembler = new DocumentAssembler()
//		  .setInputCol("text")
//		  .setOutputCol("document")
//
//		// 创建WordEmbeddingsModel
//		val wordEmbeddingsModel = WordEmbeddingsModel.pretrained("glove_100d")
//		  .setInputCols(Array("document"))
//		  .setOutputCol("embeddings")
//
//		// 转换数据
//		val documentDF = documentAssembler.transform(df)
//		val embeddingsDF = wordEmbeddingsModel.transform(documentDF)
//
//		// 创建VectorAssembler
//		val vectorAssembler = new VectorAssembler()
//		  .setInputCols(Array("embeddings"))
//		  .setOutputCol("features")
//
//		// 创建BucketedRandomProjectionLSH
//		val brp = new BucketedRandomProjectionLSH()
//		  .setBucketLength(2.0)
//		  .setNumHashTables(3)
//		  .setInputCol("features")
//		  .setOutputCol("hashes")
//
//		// 训练模型
//		val model = brp.fit(vectorAssembler.transform(embeddingsDF))
//
//		// 搜索
//		model.approxNearestNeighbors(vectorAssembler.transform(embeddingsDF), "你好", 2).show()
//
//		spark.stop()
//	}

}
