package job.local
import play.api.libs.json._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import utils.SparkJobs.LocalSparkJob
import utils.TimeOperations


object MyTest extends LocalSparkJob {
    override def run(): Unit = {

//        val simpleData = Seq(("James ", "", "Smith", "36636", "M", 3000, Array(0.0)),
//            ("Michael ", "Rose", "", "40288", "M", 4000, Array(0.0)),
//            ("Robert ", "", "Williams", "42114", "M", 4000, Array(0.0)),
//            ("Maria ", "Anne", "Jones", "39192", "F", 4000, Array(0.0)),
//            ("Jen", "Mary", "Brown", "", "F", -1, Array(0.0))
//        )
//        val df = sc.parallelize(simpleData).toDF("firstname", "middlename", "lastname", "id", "gender", "salary", "test")
        val ts = TimeOperations.getTimestamp("20211125")
        val t = TimeOperations.getDateDelta("20211125" , -1)
        println(t)

    }


}
