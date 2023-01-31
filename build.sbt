import Dependencies._
import sbtassembly.AssemblyKeys.{assembly, assemblyJarName}

addArtifact(Compile / assembly / artifact, assembly)

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
    libraryDependencies ++= dependencies,

    assembly / assemblyMergeStrategy := {
      case "reference.conf" => MergeStrategy.concat
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", "services", _@_*) => MergeStrategy.concat
      case PathList("META-INF", xs) if xs.endsWith(".SF") || xs.endsWith(".DSA") || xs.endsWith(".RSA") => MergeStrategy.discard
      case PathList("META-INF", _@_*) => MergeStrategy.first
      case _ => MergeStrategy.first
    },
    Compile / assembly / artifact := {
      val art = (Compile /  assembly / artifact).value
      art.withClassifier(Some("assembly"))
    }
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
