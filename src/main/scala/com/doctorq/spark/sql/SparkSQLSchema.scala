package com.doctorq.spark.sql

import com.doctorq.spark.ml.SparkObject
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object SparkSQLSchema extends SparkObject {

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val peopleRDD = spark.sparkContext.textFile("src/main/resources/people.txt")
    val schemaString = "name age"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)
    val rowRDD = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))
    val peopleDF = spark.createDataFrame(rowRDD, schema)
    peopleDF.createOrReplaceTempView("people")
    val results = spark.sql("select name from people")
    results.map(attributes => "Name: " + attributes(0)).show()
  }

}
