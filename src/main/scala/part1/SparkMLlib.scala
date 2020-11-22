package part1

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object SparkMLlib {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"Book example: Scala")
    val sc = new SparkContext(conf)

    // Load 2 types of emails from text files: spam and ham (non-spam)
    // Each line has text from one email.
    val spam = sc.textFile("src/main/resources/files/spam.txt")
    val normal = sc.textFile("rc/main/resources/files/ham.txt")

    // Create a HashingTF instance to map email text to vectors of 10,000 features
    val tf = new HashingTF(numFeatures = 10000)
    // Each email is split into words, and each word is mapped to one feature.
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))

    // Create LabeledPoint datasets for positive (spam) and negative (normal) examples
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples.union(negativeExamples)
    trainingData.cache() // Cache since Logistic Regression is an iterative algorithm.

    // Run Logistic Regression using the SGD algorithm
    val model = new LogisticRegressionWithSGD().run(trainingData)

    // Test on a positive example (spam) and a negative one (normal)
    val postTest = tf.transform(
      "O M G GET cheap stuff by sending money to ...".split(" "))
    val negTest = tf.transform(
      "Hi Dad, I started studying Spark the other ...".split(" "))
    println("Prediction for positive test example: " + model.predict(postTest))
    println("Prediction for negative test example: " + model.predict(negTest))

    sc.stop()


  }

}
