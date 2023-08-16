package waimai.job.local

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.summary.TextRankKeyword
import com.hankcs.hanlp.tokenizer.BasicTokenizer

import scala.collection.JavaConverters._


object NlpDemo {
    def main(args: Array[String]): Unit = {
        // HanLP.Config.enableDebug() // 为了避免你等得无聊，开启调试模式说点什么:-)


        val result = HanLP.segment("多芬樱花甜香浓密沐浴泡泡400ml").asScala
        result.foreach(x => println(x.word))

        val text = "举办纪念活动铭记二战历史，不忘战争带给人类的深重灾难，是为了防止悲剧重演，确保和平永驻；" + "铭记二战历史，更是为了提醒国际社会，需要共同捍卫二战胜利成果和国际公平正义，" + "必须警惕和抵制在历史认知和维护战后国际秩序问题上的倒行逆施。"
        System.out.println(BasicTokenizer.segment(text))
        // 测试分词速度，让大家对HanLP的性能有一个直观的认识
        val start = System.currentTimeMillis
        val pressure = 100000
        for (i <- 0 until pressure) {
            BasicTokenizer.segment(text)
        }
        val costTime = (System.currentTimeMillis - start) / 1000.toDouble
        printf("BasicTokenizer分词速度：%.2f字每秒\n", text.length * pressure / costTime)

    }


}
