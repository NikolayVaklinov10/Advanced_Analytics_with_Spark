package part1

import org.apache.spark.{SparkConf, SparkContext}

object SparkMLlib {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"Book example: Scala")
    val sc = new SparkContext(conf)

    // Load 2 types of emails from text files: spam and ham (non-spam)
    // Each line has text from one email.
    val spam = sc.textFile("src/main/resources/files/spam.txt")
    val ham = sc.textFile("rc/main/resources/files/ham.txt")



  }

}
