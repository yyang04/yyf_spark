package waimai.job.local.nlp

import com.johnsnowlabs.nlp.SparkNLP
import waimai.utils.SparkJobs.LocalSparkJob


object nlp extends LocalSparkJob {

	override def run(): Unit = {


		val spark = SparkNLP.start(aarch64 = true)






//
//		CustomDictionary.add("格力高")
//		CustomDictionary.add("百醇")
//		CustomDictionary.add("优酸乳")
//		CustomDictionary.add("脉动")
//		CustomDictionary.add("奥妙")
//
//
//		val filterNature = Set(Nature.m, Nature.nx, Nature.w, Nature.q, Nature.nz)
//		val text = "脉动维生素饮料 桃子口味 600ml/瓶"
//		val termList = HanLP.segment(text)
//		val filterTerm = termList.asScala.filter(x ⇒ !filterNature.contains(x.nature))
//		println(filterTerm)

	}

}
