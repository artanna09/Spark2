package com.accenture.bootcamp

import com.accenture.bootcamp.day1.Tasks
import com.accenture.bootcamp.day1.Loader
import com.accenture.bootcamp.day1.Tokenizer
import com.accenture.bootcamp.day1.Tokenizer.{Word, tokenize, numbers, words, wordFrequency}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ${user.name}
 */
object App {
  
 def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)


  
  def main(args : Array[String]) {
    val conf = new SparkConf()
      .setAppName("Name")
      .setMaster("local[*]")  // local mode

    val sc = new SparkContext(conf)

    //println( "Hello World!" )
    // println("concat arguments = " + foo(args))

    val textFile1: RDD[String] = Loader.loadNewYearHonours(sc)
    textFile1.foreach(println)

    val textFile2: RDD[String] = Loader.loadAustralianTreaties(sc)
    textFile2.foreach(println)
   }

}
