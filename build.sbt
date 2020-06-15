name := "fuzzyCMeans"

version := "2.4.5"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.5"
  , "org.apache.spark" %% "spark-mllib" % "2.4.5"
  , "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5"
  , "org.apache.spark" %% "spark-streaming" % "2.4.5"
  , "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.5"
  , "org.apache.spark" %% "spark-launcher" % "2.4.5"
  , "org.apache.spark" %% "spark-sql" % "2.4.5"
  , "org.scalactic" %% "scalactic" % "3.1.1"
  , "org.scalatest" %% "scalatest" % "3.1.1"
  , "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  , "com.typesafe" % "config" % "1.4.0"
  , "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
)

libraryDependencies += "org.postgresql" % "postgresql" % "42.2.8"

// enable publishing the jar produced by `test:package`
publishArtifact in(Test, packageBin) := true

retrieveManaged := true


test in assembly := {}


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}