name := "Advanced_Analytics_with_Spark"

version := "0.1"


libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.scala-lang/scala-library
  "org.scala-lang" % "scala-library" % "2.12.1",

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
 "org.apache.spark" %% "spark-mllib" % "3.0.1",

"org.apache.spark" %% "spark-sql" % "3.0.1",

  // https://mvnrepository.com/artifact/org.scalanlp/breeze
"org.scalanlp" %% "breeze" % "1.0"


)