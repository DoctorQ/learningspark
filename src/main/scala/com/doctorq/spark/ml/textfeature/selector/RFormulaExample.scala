package com.doctorq.spark.ml.textfeature.selector

import com.doctorq.spark.ml.SparkObject

object RFormulaExample extends SparkObject {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.feature.RFormula

    val dataset = spark.createDataFrame(Seq(
      (7, "US", 18, 1.0),
      (8, "CA", 12, 0.0),
      (9, "NZ", 15, 0.0)
    )).toDF("id", "country", "hour", "clicked")

    val formula = new RFormula()
      .setFormula("clicked ~ country + hour")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val output = formula.fit(dataset).transform(dataset)
    output.select("features", "label").show()
  }

}
