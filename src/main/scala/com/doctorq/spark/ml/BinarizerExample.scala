package com.doctorq.spark.ml

import org.apache.spark.ml.feature.Binarizer

object BinarizerExample extends SparkObject {

  def main(args: Array[String]): Unit = {
    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame = spark.createDataFrame(data).toDF("id", "feature")
    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.0)
    val binarizedDataFrame = binarizer.transform(dataFrame)
    println(s"Binarizer output with Threshold = ${binarizer.getThreshold}")
    binarizedDataFrame.show()
  }

}
