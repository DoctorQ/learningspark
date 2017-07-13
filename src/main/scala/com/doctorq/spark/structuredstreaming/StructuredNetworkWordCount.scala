package com.doctorq.spark.structuredstreaming

import org.apache.spark.sql.SparkSession


/**
  * Created by doctorq on 2017/7/13.
  * 利用StructuredStreaming来实现单词计算
  */
object StructuredNetworkWordCount {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()
    import spark.implicits._
    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()
    val query = wordCounts.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
  }

}
