package com.accenture.bootcamp.day1

import com.accenture.bootcamp.day1.Tokenizer.{Word, tokenize, numbers, words, wordFrequency}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


trait Tasks {

  def sc: SparkContext

  val newYearHonours: RDD[String] = Loader.loadNewYearHonours(sc)
  val australianTreaties: RDD[String] = Loader.loadAustralianTreaties(sc)

  /**
    * Task #5: How many words are in ListOfAustralianTreaties.txt?
    * Hint: use countWords() to count amount of words
    *
    * @return amount of words
    */
  def countWords(australianTreaties: RDD[Word]): Long = {
    // TODO Task #5: How many words are in ListOfAustralianTreaties.txt?
    val wordRDD = tokenize(australianTreaties)
    wordRDD.map(x=> (x,1))
      .reduceByKey(_+_)
      .map(x => x._2)
      .sum()
      .toLong
  }

  /**
    * Task #6: How many words are in both .txt files?
    * Hint: use countWords() to count amount of words
    *
    * @return amount of words in both .txt files
    */
  def task6(): Long = {
    // TODO Task #6: How many words are in both .txt files?
    val rddNewYear = tokenize(newYearHonours)
    val val1 = rddNewYear.map(x => (x,1))
    .reduceByKey(_+_)
      .map(x => x._2)
      .sum()
      .toLong
    val austrTreat = tokenize(australianTreaties)
    val val2 = austrTreat.map(x => (x,1))
      .reduceByKey(_+_)
      .map(x => x._2)
      .sum()
      .toLong
    val1 + val2
  }


  /**
    * Task #8: How many unique numbers are in ListOfAustralianTreaties.txt? 
    *
    * @return
    */


  def task8(): Long = {
    // TODO Task #8: How many unique numbers are in ListOfAustralianTreaties.txt? 
    val rddSum = numbers(tokenize(australianTreaties))
    rddSum.distinct()
      .count()
  }

  /**
    * Task #9: Calculate average of all numbers in ListOfAustralianTreaties.txt? 
    * i.e. string "1842 – Treaty 5 March 1856)[5]» has average 927
    *
    * @return average value for ListOfAustralianTreaties.txt
    */
  def task9(): Double = {
    // TODO Task #9: Calculate average of all numbers in ListOfAustralianTreaties.txt? 
    val rddAvg = numbers(tokenize(australianTreaties))
    val sum = rddAvg.sum
      .toLong
    val avg = rddAvg.count()
    sum/avg + 1
  }

  /**
    * Task #11: What are 10 most frequent symbols in ListOfAustralianTreaties.txt?
    * Hint: use wordFrequency()
    * Hint: the result should be sorted in descending way
    *
    * @return
    */
  def task11: Seq[String] = {
    // TODO Task #11: What are 10 most frequent symbols in ListOfAustralianTreaties.txt?
    val top = australianTreaties
    val cnt = top.flatMap(x => x.split(" "))
      .map(word => (word,1))
      .reduceByKey(_+_)
    val cntSwap = cnt.map(_.swap)
    val sortTop = cntSwap.sortByKey(false,0)
    sortTop.take (10)
      .map(_._2)
  }

  /**
    * Task #13: How many elements there are in each group in ListOfAustralianTreaties.txt
    * Hint: use Tokenizer.classify()
    *
    * @return
    */
  def task13: Map[String, Long] = {
    // TODO Task #13: How many elements there are in each group in ListOfAustralianTreaties.txt
    val countElem = Tokenizer.tokenize(australianTreaties)
    Tokenizer.classify(countElem)
  }

  /**
    * Task #14: Print samples of each group with A, B, C, D values from ListOfAustralianTreaties.txt?
    * Hint: use Tokenizer.wordClassifier()
    * @return
    */
  def task14: RDD[(String, (Int, Int, Int, Int))] = {
    // TODO Task #14: Print samples of each group with A, B, C, D values from ListOfAustralianTreaties.txt?
    Tokenizer.tokenize(australianTreaties)
      .map(word => (Tokenizer.wordClassifier(word), Tokenizer.wordStats(word)))
  }

}
