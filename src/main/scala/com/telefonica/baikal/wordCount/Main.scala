package com.telefonica.baikal.wordCount

import org.apache.commons.cli.MissingArgumentException
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}

class Arguments(arguments: Seq[String]) extends ScallopConf(arguments) {
  val from: ScallopOption[String] = opt[String] (required = true)
  val to: ScallopOption[String] = opt[String] (required = true)
  val master: ScallopOption[String] = opt[String] (required = false)
  verify()
}

object Main {

  val EnvSaName: String = "WC_SA_ACCOUNT"
  val EnvSaKey: String = "WC_SA_KEY"

  protected def buildSparkSession(sparkMaster: Option[String] = None): SparkSession = {
    val sparkBuilder = SparkSession.builder()
      .appName("Word Count Algorithm")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config(s"fs.azure.account.key.${sys.env.getOrElse(EnvSaName, "")}.blob.core.windows.net", sys.env.getOrElse(EnvSaKey, ""))
      .config("fs.AbstractFileSystem.wasb.Impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      .config("fs.AbstractFileSystem.wasbs.Impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure")

    val spark = sparkMaster.map(master => sparkBuilder.master(master))
      .getOrElse(sparkBuilder)
      .getOrCreate()

    spark
  }

  // run --from=entrada --to=salida
  def main(args: Array[String]): Unit = {
    /*
    val spark = SparkSession
      .builder()
      .appName("Word Count Algorithm")
      .master("local[*]")
      .getOrCreate()
    */

    val input = new Arguments(args)
    val in = input.from()
    val out = input.to()
    val spark = buildSparkSession(input.master.toOption)

    try {
        WordCount.WordCountIntoFile(spark, in, out)
    } finally {
      spark.close()
    }
  }
}
