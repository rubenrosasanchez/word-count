package com.telefonica.baikal.wordCount

import java.io.File
import java.nio.file.Files
import scala.reflect.io.Directory

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WordCountTest extends AnyFlatSpec with Matchers {

  //private val quijoteFilePath = getClass.getResource("/data/quijote").toURI.toString
  private val otroQuijoteFilePath = getClass.getResource("/data/otro-quijote").toURI.toString

  private val firstQuijotePath = getClass.getResource("/data/basic-quijote").toURI.toString
  private val singleLinePath = getClass.getResource("/data/single-line").toURI.toString
  private val multipleLinePath = getClass.getResource("/data/multiple-line").toURI.toString

  private val outDirSingleLine = System.getProperty("user.dir") + "/out/single-line" //"/data/output/single-line-result"
  private val outDirMultipleLine = System.getProperty("user.dir") + "/out/multiple-line" //"/data/output/single-line-result"
  private val outDirQuijote = System.getProperty("user.dir") + "/out/quijote" //"/data/output/single-line-result"


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

  // testOnly *.WordCountTest -- -z "check the single-line file test on Dataframe"
  it should "check the single-line file test on Dataframe" in {
    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()

    val res = WordCount.run(spark, singleLinePath)

    val listRes = res.collect().toSeq
    val expectedResult = List(Row("three", 3), Row("two", 2), Row("one", 1))

    listRes should be (expectedResult)
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

    WordCount.WordCountIntoFile(spark, singleLinePath, temporaryDirectory) // completeTestPath

    val filesInPath: List[File] = getListOfFiles(temporaryDirectory) // completeTestPath
    val file = getFileAsString(filesInPath)
    if (file.isEmpty) {
      emptyFile = true
    }

    file should be (expectedResult)
    emptyFile should be (false)
  }

  // testOnly *.WordCountTest -- -z "multiple-line test on Dataframe"
  it should "check the multiple-line file test on Dataframe" in {
    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()

    val res = WordCount.run(spark, multipleLinePath)

    val listRes = res.collect().toSeq
    val expectedResult = Array(
      Row("four", 4), Row("three", 3), Row("two", 2), Row("one", 1), Row("qwerty", 1), Row("asdf", 1)
    )

    listRes should be (expectedResult)
    /*
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

    WordCount.WordCountIntoFile(spark, multipleLinePath, temporaryDirectory) // completeTestPath

    val filesInPath: List[File] = getListOfFiles(temporaryDirectory) // completeTestPath
    val file = getFileAsString(filesInPath)
    if (file.isEmpty) {
      emptyFile = true
    }

    file should be (expectedResult)
    emptyFile should be (false)

  }

  // testOnly *.WordCountTest -- -z "get word-count of single-line file as Map"
  it should "get word-count of single-line file as Map" in {
    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()

    val res = WordCount.getWordCountMap(spark, singleLinePath)
    val expectedResult = Map("three" -> 3, "two" -> 2, "one" -> 1)
    val diff = (res.keySet -- expectedResult.keySet) ++ (expectedResult.keySet -- res.keySet)

    diff.isEmpty should be (true)
  }
}