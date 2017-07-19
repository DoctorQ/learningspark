package com.doctorq.spark.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}


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
    val query = wordCounts.writeStream.queryName("doctorq").outputMode("update").format("memory").start()
    //    println(s"id: ${query.id}") // get the unique identifier of the running query that persists across restarts from checkpoint data
    //
    //    println(s"runId: ${query.runId}") // get the unique id of this run of the query, which will be generated at every start/restart
    //
    //    println(s"name: ${query.name}") // get the name of the auto-generated or user-specified name
    //
    //    query.explain() // print detailed explanations of the query

    //    spark.sql("select * from doctorq").show()

    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
      }
    })

    query.awaitTermination()
  }

}
