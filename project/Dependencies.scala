import Dependencies.sparkSql
import sbt._

object Dependencies {
  val sparkVersion = "3.1.2"
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.11"

  lazy val dependencies = Seq(
      sparkCore % Provided,
      sparkSql % Provided,

      sparkCore % Test,
      sparkSql % Test,
      scalaTest % Test
  )
}
