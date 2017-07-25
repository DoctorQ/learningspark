package com.doctorq.spark.ml

import org.apache.spark.ml.feature.StopWordsRemover

/**
  * 停词器
  */
object StopWordsRemoverExample extends SparkObject{

  def main(args: Array[String]): Unit = {
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show(false)

  }

}
