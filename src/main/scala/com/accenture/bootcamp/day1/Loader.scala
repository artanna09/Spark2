package com.accenture.bootcamp.day1

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Loader {

  protected def fromResource(resource: String): String = {
    new java.io.File("src/test/resources/" + resource).getCanonicalPath
  }

  def loadNewYearHonours(sc: SparkContext): RDD[String] = {
    // TODO Task #1: Create RDD from file `1918NewYearHonours.txt`
    val filePath: String = fromResource("1918NewYearHonours.txt")
    val textFile1: RDD[String] = sc.textFile(filePath)
    textFile1
  }

  def loadAustralianTreaties(sc: SparkContext): RDD[String] = {
    // TODO Task #2: Create RDD from file `ListOfAustralianTreaties.txt`
    val filePath: String = fromResource("ListOfAustralianTreaties.txt")
    val textFile2: RDD[String] = sc.textFile(filePath)
    textFile2
  }


}
