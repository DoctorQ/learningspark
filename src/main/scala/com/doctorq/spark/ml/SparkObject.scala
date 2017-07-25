package com.doctorq.spark.ml

import org.apache.spark.sql.SparkSession

trait SparkObject {

  val spark = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

}
