package com.telefonica.baikal.wordCount

import scala.language.postfixOps

import org.apache.spark.sql.functions.{col, first, lit}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object WordCount {

  case class WordRef(word: String, count: Long)

  val regex: String = "[^a-zà-úä-üA-ZÀ-ÚÄ-Ü]+"
  // "[,.;:¿?¡!\\/#$%<·>ºª^&*{}=\\-_`~()”“\"…\\[\\]]"
  def run(sparkSession: SparkSession, path: String): DataFrame = {
    import sparkSession.implicits._

    sparkSession.read.text(path)
      .as[String]
      .map(_.replaceAll(regex, " "))
      .flatMap(_.split(" "))
      .map(m => WordRef(m.toLowerCase(), 1L))
      .filter($"word" =!= " " && $"word" =!= "")
      .groupBy($"word")
      .count()
      .orderBy(col("count").desc)
  }

  def getWordCountMap(sparkSession: SparkSession, path: String): Map[String, Long] = {
    import sparkSession.implicits._

    sparkSession.read.text(path)
      .as[String]
      .map(_.replaceAll(regex, " "))
      .flatMap(_.split(" "))
      .map(m => WordRef(m.toLowerCase(), 1L))
      .filter($"word" =!= " " && $"word" =!= "")
      .groupBy($"word")
      .count()
      .orderBy(col("count").desc)
      .as[WordRef]
      .collect().map(r => r.word -> r.count).toMap
  }

  def WordCountIntoFile(sparkSession: SparkSession, inputPath: String, outputPath: String): Unit = {
    import sparkSession.implicits._

    sparkSession.read.text(inputPath)
      .as[String]
      .map(_.replaceAll(regex, " "))
      .flatMap(_.split(" "))
      .map(m => WordRef(m.toLowerCase(), 1L))
      .filter($"word" =!= " " && $"word" =!= "")
      .groupBy($"word")
      .count()
      .orderBy(col("count").desc)
      .coalesce(1).write.mode("overwrite").csv(outputPath)
  }
}
