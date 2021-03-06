package com.doctorq.spark.ml.classification

import com.doctorq.spark.ml.SparkObject

object MulticlassLogisticRegressionWithElasticNetExample extends SparkObject {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.classification.LogisticRegression

    // Load training data
    val training = spark
      .read
      .format("libsvm")
      .load("data/mllib/sample_multiclass_classification_data.txt")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for multinomial logistic regression
    println(s"Coefficients: \n${lrModel.coefficientMatrix}")
    println(s"Intercepts: ${lrModel.interceptVector}")
  }

}
