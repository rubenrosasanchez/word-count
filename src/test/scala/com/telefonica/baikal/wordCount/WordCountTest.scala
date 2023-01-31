package com.telefonica.baikal.wordCount

import java.io.File
import java.nio.file.Files
import scala.reflect.io.Directory

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WordCountTest extends AnyFlatSpec with Matchers {

  private val singleLinePath = getClass.getResource("/data/single-line").toURI.toString
  private val multipleLinePath = getClass.getResource("/data/multiple-line").toURI.toString

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getFileAsString(files: List[File]): String = {
    files.foreach{ f =>
      if(f.getAbsolutePath.endsWith(".csv")) {
        val source = scala.io.Source.fromFile(f.toString)
        val lines = try source.mkString finally source.close()
        return lines
      }
    }
    ""
  }

  // testOnly *.WordCountTest -- -z "single-line file with csv output"
  it should "check the single-line file with csv output file" in {
    var emptyFile = false
    val expectedResult: String = "three,3\ntwo,2\none,1\n"
    /* Expected result:
    +-----+-----+
    | word|count|
    +-----+-----+
    |three|    3|
    |  two|    2|
    |  one|    1|
    +-----+-----+
   */
    val temporaryDirectory = Files.createTempDirectory("output").toString

    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()

    WordCount.run(spark, singleLinePath, temporaryDirectory) // completeTestPath

    val filesInPath: List[File] = getListOfFiles(temporaryDirectory) // completeTestPath
    val file = getFileAsString(filesInPath)
    if (file.isEmpty) {
      emptyFile = true
    }

    file should be (expectedResult)
    emptyFile should be (false)
  }

  // testOnly *.WordCountTest -- -z "check multiple-line file with output in csv file"
  it should "check multiple-line file with output in csv file" in {
    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()

    var emptyFile = false
    val expectedResult: String = "four,4\nthree,3\ntwo,2\none,1\nqwerty,1\nasdf,1\n"//"three,3\ntwo,2\none,1\n"

    /*
    Expected result:
     +------+-----+
     |  word|count|
     +------+-----+
     |  four|    4|
     | three|    3|
     |   two|    2|
     |qwerty|    1|
     |  asdf|    1|
     |   one|    1|
     +------+-----+
    */

    val temporaryDirectory = Files.createTempDirectory("output").toString

    WordCount.run(spark, multipleLinePath, temporaryDirectory)

    val filesInPath: List[File] = getListOfFiles(temporaryDirectory)
    val file = getFileAsString(filesInPath)
    if (file.isEmpty) {
      emptyFile = true
    }

    file should be (expectedResult)
    emptyFile should be (false)

  }


}