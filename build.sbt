name := "ElasticSearchPush"

version := "1.0"

scalaVersion := "2.11.7"


// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.0"
libraryDependencies += "com.sun.jersey" % "jersey-servlet" % "1.19" excludeAll ExclusionRule(organization = "javax.servlet")



libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.2",
  "org.mongodb" %% "casbah" % "3.1.0",
  "com.databricks" %% "spark-csv" % "1.3.0",
  "com.typesafe.play" % "play-json_2.11" % "2.5.0" exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.elasticsearch" % "elasticsearch" % "2.3.0"
)


libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.3-1100-jdbc4",
  "com.typesafe.slick" %% "slick" % "2.1.0",
  "org.slf4j" % "slf4j-nop" % "1.6.4"
)


libraryDependencies ++= Dependencies.sparkAkkaHadoop

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
