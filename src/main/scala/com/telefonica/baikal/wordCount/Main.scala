package com.telefonica.baikal.wordCount

import com.telefonica.baikal.wordCount.WordCount.getClass
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}

object Main {

  //private val quijoteFilePath = getClass.getResource("/data/quijote/el_quijote.txt")
  //private val multipleLinePath = getClass.getResource("/data/multiple-line/basic.txt")

  // scallop
  class Arguments(arguments: Seq[String]) extends ScallopConf(arguments) {
    val from: ScallopOption[String] = opt[String] (required = true)
    val to: ScallopOption[String] = opt[String] (required = true)
    verify()
  }

  // run --from=entrada --to=salida
  // run --from /Users/cxb0514/Developer/4P/quijote-word-count-exercise/word-count/src/test/resources/data/single-line/basic.txt --to /Users/cxb0514/Developer/4P/quijote-word-count-exercise/word-count/out
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()


    //val singleLinePath = spark.sparkContext.textFile("src/test/resources/data/single-line/basic.txt").getClass.toString
    //WordCount.run(spark, singleLinePath)

    //args.foreach(a => println("Argument: " + a))

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
