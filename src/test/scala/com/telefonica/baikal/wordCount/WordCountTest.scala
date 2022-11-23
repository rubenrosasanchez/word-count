package com.telefonica.baikal.wordCount

import java.io.File
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

  // testOnly *.WordCountTest -- -z "single-line test on Dataframe"
  it should "check the single-line file test on Dataframe" in {
    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()

    val res = WordCount.run(spark, singleLinePath)
    println("\t\tResult in test single line path")
    res.show()

    val listRes = res.collect().toSeq
    val expectedResult = List(Row("three", 3), Row("two", 2), Row("one", 1))

    listRes should be (expectedResult)

    /*
      +-----+-----+
      | word|count|
      +-----+-----+
      |three|    3|
      |  two|    2|
      |  one|    1|
      +-----+-----+
     */
  }

  // testOnly *.WordCountTest -- -z "single-line file with csv output"
  it should "check the single-line file with csv output file" in {
    val tempOut = getClass.getResource("/output/").toURI.toString
      .split(":").toSeq.last
    val testType = "single-line/"
    val completeTestPath = tempOut + testType
    var emptyFile = false
    val expectedResult: String = "three,3\ntwo,2\none,1\n"

    /*
    +-----+-----+
    | word|count|
    +-----+-----+
    |three|    3|
    |  two|    2|
    |  one|    1|
    +-----+-----+
   */

    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()

    WordCount.countIntoFile(spark, singleLinePath, completeTestPath)

    val filesInPath: List[File] = getListOfFiles(completeTestPath)
    val file = getFileAsString(filesInPath)
    if (file.isEmpty) {
      emptyFile = true
    }

    println("completeTestPath == " + completeTestPath)

    //val directory = new Directory(new File(completeTestPath))
    //directory.deleteRecursively()

    file should be (expectedResult)
    emptyFile should be (false)

  }

  def getFileAsString(files: List[File]): String = {
    println("Dentro del getFileAsString:")
    files.foreach{ f =>
      println("Dentro del foreach: f = " + f.toURI.toString)
      if(f.getAbsolutePath.endsWith(".csv")) {
        val source = scala.io.Source.fromFile(f.toString)
        val lines = try source.mkString finally source.close()
        return lines
      }
    }
    ""
  }

  /*
  Dentro del getFileAsString:
Dentro del foreach: f = file:/Users/cxb0514/Developer/4P/word-count/quijote-word-count/target/scala-2.12/classes/output/single-line/._SUCCESS.crc
Dentro del foreach: f = file:/Users/cxb0514/Developer/4P/word-count/quijote-word-count/target/scala-2.12/classes/output/single-line/.part-00000-1e65609c-3488-4585-b48b-489d1f72d138-c000.csv.crc
Dentro del foreach: f = file:/Users/cxb0514/Developer/4P/word-count/quijote-word-count/target/scala-2.12/classes/output/single-line/part-00000-1e65609c-3488-4585-b48b-489d1f72d138-c000.csv
Dentro del csv:
/Users/cxb0514/Developer/4P/word-count/quijote-word-count/target/scala-2.12/classes/output/single-line/part-00000-1e65609c-3488-4585-b48b-489d1f72d138-c000.csv
Dentro del foreach: f = file:/Users/cxb0514/Developer/4P/word-count/quijote-word-count/target/scala-2.12/classes/output/single-line/_SUCCESS
File is empty! =

  */

  // testOnly *.WordCountTest -- -z "multiple-line test on Dataframe"
  it should "check the multiple-line file test on Dataframe" in {
    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()

    val res = WordCount.run(spark, multipleLinePath)
    println("\t\tResult in test multiple line path")
    res.show()

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


  // testOnly *.WordCountTest -- -z "multiple-line file with output in csv file"
  it should "check multiple-line file with output in csv file" in {
    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()

    WordCount.countIntoFile(spark, multipleLinePath, outDirMultipleLine)
    println("\t\tResult in test single line path")

    true should be (true)

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


  // testOnly *.WordCountTest -- -z "quijote"
  it should "word-count of quijote file" in {
    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()

    val res = WordCount.run(spark, otroQuijoteFilePath)
    println("\t\tResult in test multiple line path")
    res.show()
    true should be (true)
  }

  // testOnly *.WordCountTest -- -z "quijote file with output in csv file"
  it should "word-count of quijote into csv output file" in {
    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()

    WordCount.countIntoFile(spark, otroQuijoteFilePath, outDirQuijote)
    println("\t\tResult in test single line path")
    true should be (true)
  }

  // testOnly *.WordCountTest -- -z "Map"
  it should "get word-count of single-line file as Map" in {
    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()

    val res = WordCount.getWordCountMap(spark, singleLinePath)
    println("\t\tResult in test single line path")
    println("Map of single-line file is: \n\t" + res.toString())


    true should be (true)
  }

}