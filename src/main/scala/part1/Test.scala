package part1

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object Test extends App {

  val spark = SparkSession.builder().appName("Testing").master("local[2]").getOrCreate()

  val sc = spark.sparkContext
  val input = sc.textFile("src/main/resources/data/input.txt")
  val tokenized2 = input.map(line => line.split(" "))
    .filter(word => word.size > 0)

  val counts = tokenized2.map(word => (word(0), 1))
    .reduceByKey{ (a,b) => a + b }

  input.toDebugString










}
