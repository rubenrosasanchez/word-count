package com.telefonica.baikal.wordCount

import com.telefonica.baikal.wordCount.WordCount.getClass
import org.apache.spark.sql.SparkSession

object Main {

  private val quijoteFilePath = getClass.getResource("/data/quijote/el_quijote.txt")

  // = getClass.getResource("/data/single-line/basic.txt")//.toURI.toString
  private val multipleLinePath = getClass.getResource("/data/multiple-line/basic.txt")


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()

    val singleLinePath = spark.sparkContext.textFile("src/test/resources/data/single-line/basic.txt").getClass.toString
    WordCount.run(spark, singleLinePath)
  }

}
