package part1

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.SparkContext._


object SparkStreaming {

  // Create a local StreamingContext with two working thread and batch interval of 1 second

  def wordsCount() {
    // The master requires 2 cores to prevent a starvation scenario
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("NetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(1))

    // Create a DSteam that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    // to start everything
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    wordsCount()
  }

}
