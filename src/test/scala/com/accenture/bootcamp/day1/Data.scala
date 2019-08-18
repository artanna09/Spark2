package com.accenture.bootcamp.day1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Data {

  val spark: SparkSession = SparkSession.builder()
    .appName("BigDataBootcampDay1")
    .master("local")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  val newYearHonours: RDD[String] = Loader.loadNewYearHonours(spark.sparkContext)
  val australianTreaties: RDD[String] = Loader.loadAustralianTreaties(spark.sparkContext)



}
