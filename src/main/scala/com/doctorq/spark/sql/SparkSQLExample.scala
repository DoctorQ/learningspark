package com.doctorq.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by doctorq on 2017/7/17.
  */
object SparkSQLExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSQLExample")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json("src/main/resources/people.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()

    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people where age > 19")
    sqlDF.show()

    df.createGlobalTempView("people")
    spark.sql("select * from global_temp.people where name='Andy'").show()
    // create dataset
    val caseClassDS = Seq(Person("Doctorq", 32)).toDS()
    caseClassDS.show()
    val primitiveDS = Seq(1, 2, 3).toDS()
    val d = primitiveDS.map(_ + 1).collect()
    val path = "src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
  }

  case class Person(name: String, age: Long)

}
