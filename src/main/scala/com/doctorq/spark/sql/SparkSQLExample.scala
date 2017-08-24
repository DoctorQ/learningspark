package com.doctorq.spark.sql

import com.doctorq.spark.ml.SparkObject
import org.apache.spark.sql.SparkSession

/**
  * Created by doctorq on 2017/7/17.
  */
object SparkSQLExample extends SparkObject {

  def main(args: Array[String]): Unit = {
    //    val spark = SparkSession
    //      .builder()
    //      .appName("SparkSQLExample")
    //      .config("spark.some.config.option", "some-value")
    //      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json("src/main/resources/people.json")
    //df.show()
    df.printSchema()
    //基于df的查询方法
    // df.select("name").show()
    //df.select($"name", $"age" + 1).show()
    //df.filter($"age" > 21).show()
    //df.groupBy("age").count().show()

    //创建临时表,只存在于当前session
    df.createOrReplaceTempView("people")
    //创建临时表后就可以使用sql语句了
    val sqlDF = spark.sql("select * from people")
    //sqlDF.show()
    //创建全局临时表
    df.createGlobalTempView("people")
    val gsqlDF = spark.sql("select * from global_temp.people")
    //gsqlDF.show()

    spark.newSession().sql("select * from global_temp.people")
    //    spark.newSession().sql("select * from people")
    //使用DataSet
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    //caseClassDS.show()

    //encoders
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect()

    val peopleDS = df.as[Person]
    // df.show()
    // peopleDS.show()

    //RDD convert DataFrame
    val peopleDF = spark.sparkContext.textFile("src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    peopleDF.createOrReplaceTempView("people")
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    teenagersDF.show()
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
  }

  case class Person(name: String, age: Long)

}
