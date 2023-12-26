package waimai.job.local.nlp

import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.annotators.ner.NerConverter
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.util.Benchmark
import org.apache.spark.ml.Pipeline
import waimai.utils.SparkJobs.LocalSparkJob


object NerDLPipeline extends LocalSparkJob {

	override def run(): Unit = {
		spark.sparkContext.setLogLevel("WARN")
		val document = new DocumentAssembler()
		  .setInputCol("text")
		  .setOutputCol("document")

		val token = new Tokenizer()
		  .setInputCols("document")
		  .setOutputCol("token")

		val normalizer = new Normalizer()
		  .setInputCols("token")
		  .setOutputCol("normal")

		val wordEmbeddings = WordEmbeddingsModel.pretrained()
		  .setInputCols("document", "token")
		  .setOutputCol("word_embeddings")

		val ner = NerDLModel.pretrained()
		  .setInputCols("normal", "document", "word_embeddings")
		  .setOutputCol("ner")

		val nerConverter = new NerConverter()
		  .setInputCols("document", "normal", "ner")
		  .setOutputCol("ner_converter")

		val finisher = new Finisher()
		  .setInputCols("ner", "ner_converter")
		  .setIncludeMetadata(true)
		  .setOutputAsArray(false)
		  .setCleanAnnotations(false)
		  .setAnnotationSplitSymbol("@")
		  .setValueSplitSymbol("#")

		val pipeline = new Pipeline().setStages(Array(document, token, normalizer, wordEmbeddings, ner, nerConverter, finisher))

		val testing = Seq(
			(1, "Google is a famous company"),
			(2, "Peter Parker is a super heroe")
		).toDS.toDF("_id", "text")

		val result = pipeline.fit(Seq.empty[String].toDS.toDF("text")).transform(testing)
		Benchmark.time("Time to convert and show") {
			result.select("ner", "ner_converter").show(truncate = false)
		}
	}
}