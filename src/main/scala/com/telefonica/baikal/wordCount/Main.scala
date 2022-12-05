package com.telefonica.baikal.wordCount

import com.telefonica.baikal.wordCount.WordCount.getClass
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}

object Main {

  // scallop args
  class Arguments(arguments: Seq[String]) extends ScallopConf(arguments) {
    val from: ScallopOption[String] = opt[String] (required = true)
    val to: ScallopOption[String] = opt[String] (required = true)
    verify()
  }

  // run --from=entrada --to=salida
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()

    try {
      val input = new Arguments(args)
      val in = input.from.getOrElse("")
      val out = input.to.getOrElse("")
      println("input => From:" + in + " \n\t To: " + out)

      if(in.nonEmpty && out.nonEmpty) {
        WordCount.WordCountIntoFile(spark, in, out)
      }
    } finally {
      spark.close()
    }



  }

}
