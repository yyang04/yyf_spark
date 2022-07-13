package privacy_clustering

import utils.SparkJob

object GhsClustering extends SparkJob {
    def main(args: Array[String]): Unit = {
        super.initSpark(this.getClass.getName, args)




    }
}
