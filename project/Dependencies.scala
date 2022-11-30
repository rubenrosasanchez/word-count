//import Dependencies.sparkSql
//import coursier.core.Configuration.provided
import sbt._

object Dependencies {
  val sparkVersion = "3.1.2"
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion //% provided
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.11"

  val scallop = "org.rogach" %% "scallop" % "4.1.0"

  lazy val dependencies = Seq(
    sparkCore,// % Provided,
    sparkSql,// % Provided,
    scallop,

    sparkCore % Test,
    sparkSql % Test,
    scalaTest % Test,
  )
}
