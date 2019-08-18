package com.accenture.bootcamp.day1

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

trait SparkSuite extends FunSuite with Matchers with BeforeAndAfterAll with Tasks{
  def spark: SparkSession = Data.spark
  def sc: SparkContext = Data.spark.sparkContext

  override protected def afterAll(): Unit = {
    spark.close()
  }


}
