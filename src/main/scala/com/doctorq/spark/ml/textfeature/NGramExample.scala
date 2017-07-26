package com.doctorq.spark.ml.textfeature

import com.doctorq.spark.ml.SparkObject
import org.apache.spark.ml.feature.NGram

object NGramExample extends SparkObject {

  def main(args: Array[String]): Unit = {
    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("id", "words")

    val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")

    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.select("ngrams").show(false)
  }

}
