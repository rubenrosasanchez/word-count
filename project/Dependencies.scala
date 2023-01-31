import sbt._

object Dependencies {
  val sparkVersion = "3.1.2" //3.1.3
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion //% provided
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

  val hadoopVersion = "3.2.2"
  val hadoopAzure = "org.apache.hadoop"% "hadoop-azure"% hadoopVersion exclude("org.slf4j", "slf4j-log4j12")

  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.11"

  val scallop = "org.rogach" %% "scallop" % "4.1.0"


  lazy val dependencies = Seq(
    sparkCore, // % Provided,
    sparkSql, // % Provided,
    scallop,
    hadoopAzure,

    sparkCore % Test,
    sparkSql % Test,
    scalaTest % Test,
  )
}
