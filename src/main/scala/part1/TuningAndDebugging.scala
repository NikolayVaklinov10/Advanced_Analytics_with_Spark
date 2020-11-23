package part1

import org.apache.spark.sql.SparkSession

object TuningAndDebugging extends App {

  val spark = SparkSession.builder()
    .appName("Testing")
    .master("local[2]")
    .getOrCreate()

  val sc = spark.sparkContext
  val input = sc.textFile("input.txt")
  val tokenized = input.map(line => line.split(" ")).
    filter(word => word.size > 0)
  // Extract the first word from each line (the log level) and do a count
  val counts = tokenized.map(word => (word(0), 1)).
    reduceByKey{ (a,b) => a + b }

  //Spark UI is the first option for starting the debugging
  //

}
