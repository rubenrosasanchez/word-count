package com.telefonica.baikal.wordCount

import scala.language.postfixOps

import org.apache.spark.sql.functions.{col, first, lit}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object WordCount {

  case class WordRef(word: String, count: Long)

  def run(sparkSession: SparkSession, path: String): DataFrame /*Map[String, Long]*/ = {
    import sparkSession.implicits._

    sparkSession.read.text(path)
      .as[String]
      .map(_.replaceAll("[.,¡¿?\\/#!$%^&*;:{}=\\-_`~()”“\"…]", " "))
      .flatMap(_.split(" "))
      .map(m => WordRef(m.toLowerCase(), 1L))
      .filter($"word" =!= " " && $"word" =!= "")
      .groupBy($"word")
      .count()
      .orderBy(col("count").desc)
  }

  def getWordCountMap(sparkSession: SparkSession, path: String): Map[String, Long] = {
    import sparkSession.implicits._

    val dsr = sparkSession.read.text(path)
      .as[String]
      .map(_.replaceAll("[.,¿¡?\\/#!$%^&*;:{}=\\-_`~()”“\"…]", " "))
      .flatMap(_.split(" "))
      .map(m => WordRef(m.toLowerCase(), 1L))
      .filter($"word" =!= " " && $"word" =!= "")
      .groupBy($"word")
      .count()
      .orderBy(col("count").desc)
      .as[WordRef]

    //dsr.collect().map(r => Map(dsr.columns.zip(r.toSeq):_*))
    dsr.collect().map(r => r.word -> r.count).toMap
  }

  // Output: escribir en un csv
  def countIntoFile(sparkSession: SparkSession, inputPath: String, outputPath: String): Unit /*Map[String, Long]*/ = {
    import sparkSession.implicits._

    val data = sparkSession.read.text(inputPath)
      .as[String]
      .map(_.replaceAll("[.,¿¡?\\/#!$%^&*;:{}=\\-_`~()”“\"…]", " "))
      .flatMap(_.split(" "))
      .map(m => WordRef(m.toLowerCase(), 1L))
      .filter($"word" =!= " " && $"word" =!= "")
      .groupBy($"word")
      .count()
      .orderBy(col("count").desc)

    data.coalesce(1).write.csv(outputPath)
  }
}
