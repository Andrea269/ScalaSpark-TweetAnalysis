name := "ScalaSpark-TweetAnalysis"

version := "0.1"

scalaVersion := "2.11.8"


libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"


// https://mvnrepository.com/artifact/org.twitter4j/twitter4j-core
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.7"

// https://mvnrepository.com/artifact/org.twitter4j/twitter4j-stream
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.5"

// https://mvnrepository.com/artifact/org.apache.bahir/spark-streaming-twitter
libraryDependencies += "org.apache.bahir" % "spark-streaming-twitter_2.11" % "2.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.4.0"
