import sbt._

object Version {
  val akka      = "2.3.9"
  val hadoop    = "2.6.1"
  val logback   = "1.1.2"
  val mockito   = "1.10.19"
  val scala     = "2.11.7"
  val scalaTest = "2.2.4"
  val slf4j     = "1.7.6"
  val spark     = "1.6.0"
  val sparkES   = "2.2.0"
  val ESHadoop   = "2.1.1"
}

object Library {
  val hadoopClient   = "org.apache.hadoop" %  "hadoop-client"   % Version.hadoop
  val logbackClassic = "ch.qos.logback"    %  "logback-classic" % Version.logback
  val slf4jApi       = "org.slf4j"         %  "slf4j-api"       % Version.slf4j
  val elasticsearchSpark  = "org.elasticsearch" %% "elasticsearch-spark" % Version.sparkES
  val elasticsearchHadoop  = "org.elasticsearch" % "elasticsearch-hadoop" % Version.ESHadoop

}

object Dependencies {

  import Library._

  val sparkAkkaHadoop = Seq(
    hadoopClient,
    elasticsearchSpark
  )
}