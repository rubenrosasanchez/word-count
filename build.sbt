import Dependencies._

lazy val root = (project in file("."))
  .settings(
    inThisBuild(
      List(
        scalaVersion := "2.12.12",
        version := "0.1.0-SNAPSHOT",
        organization := "com.telefonica.baikal",
        publishMavenStyle := true
      )
    ),
    name := "word-count",
    libraryDependencies ++= dependencies
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
