package org.cakesolutions.spark

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2


fun main(args: Array<String>) {
    val inFile = args[0]
    val outFile = args[1]

    val configuration = SparkConf().setAppName("wordCountProblem")
    val sc = JavaSparkContext(configuration)

    val input = sc.textFile(inFile)
    val words = input.flatMap { x -> x.splitBy(" ") }

    val ans = words.mapToPair { x -> Tuple2(x, 1) }.reduceByKey { x, y -> x + y }

    ans.saveAsTextFile(outFile)
}
