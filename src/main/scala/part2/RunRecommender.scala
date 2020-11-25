package part2

import org.apache.spark.sql.SparkSession

object RunRecommender {

  // An application for music recommendations
  val spark = SparkSession.builder().getOrCreate()
  // Optional, but may help avoid errors due to long lineage
  spark.sparkContext.setCheckpointDir("hdfs:///tmp/")

  val base = "hdfs:///user/ds/"
  val rawUserArtistData = spark.read.textFile(base + "user_artist_data.txt")
  val rawArtistData = spark.read.textFile(base + "artist_data.txt")
  val rawArtistAlias = spark.read.textFile(base + "artist_alias.txt")




}
