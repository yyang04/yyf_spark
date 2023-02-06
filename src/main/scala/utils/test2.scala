package utils

object test2 {
    def main(args: Array[String]): Unit = {

        val rel = Set(1L, 2L, 3L)
        val predict = Array(10L, 2L, 2L, 2L, 3L, 5L)
        val k = Array(1, 2, 3)
        val a = new RecallEvaluation(rel, predict, k)
        a.print_entity()
    }

}
