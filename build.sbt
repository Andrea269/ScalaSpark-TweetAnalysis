name := "ScalaSpark-TweetAnalysis"

version := "0.1"

scalaVersion := "2.11.8"

mainClass in (Compile, packageBin) := Some("ScalaTweetAnalysis7")

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))
//libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"


val sparkVersion = "2.0.2"

libraryDependencies ++= Seq(
  "org.twitter4j" % "twitter4j-core" % "4.0.6",
  "org.twitter4j" % "twitter4j-stream" % "4.0.6",
 // "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.bahir" %% "spark-streaming-twitter" % sparkVersion
)

//javaOptions += "-XX:MaxPermSize=4048m"

//javaOptions += "-Xmx4048m"

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheUnzip = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName == "compile-0.1.0.jar"}
}