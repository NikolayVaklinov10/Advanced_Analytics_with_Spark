package part2

import org.apache.spark.sql.{Dataset, SparkSession}

object RunRecommender {

  def main(args: Array[String]): Unit = {
  // An application for music recommendations
  val spark = SparkSession.builder().getOrCreate()
  // Optional, but may help avoid errors due to long lineage
  spark.sparkContext.setCheckpointDir("hdfs:///tmp/")

  val base = "src/main/resources/profiledata_06-May-2005"
  val rawUserArtistData = spark.read.textFile(base + "user_artist_data.txt")
  val rawArtistData = spark.read.textFile(base + "artist_data.txt")
  val rawArtistAlias = spark.read.textFile(base + "artist_alias.txt")

  }

}


class RunRecommender(private val spark: SparkSession) {

  import spark.implicits._

  def preparation(
                 rawUserArtistData: Dataset[String],
                 rawArtistData: Dataset[String],
                 rawArtistAlias: Dataset[String]): Unit = {
    rawArtistData.take(5).foreach(println)

    val userArtistDF = rawUserArtistData.map{ line =>
      val Array(user, artist, _*) = line.split(' ')
      (user.toInt, artist.toInt)
    }.toDF("user", "artist")


  }


}
