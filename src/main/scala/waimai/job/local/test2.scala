package waimai.job.local
import scala.collection.JavaConverters._
import utils.TairUtil


object test2 {
    def main(args: Array[String]): Unit = {
        val data = Map("activity_spu_tab_apply_7295593" -> "{\"lemon\":[1610668916,1610783990,1610857961],\"peach\":[1609043463,1611658604]}").asJava
        TairUtil.batchPutStringTest(data)
    }

}
