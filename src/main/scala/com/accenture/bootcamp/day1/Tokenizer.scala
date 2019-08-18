package com.accenture.bootcamp.day1

import org.apache.spark.rdd.RDD

object Tokenizer {

  type Word = String
  type Classifier = String
  type Amount = Long
  type WordStats = (Int, Int, Int, Int)



  /**
    * Task #3: Tokenize (split string into words) 
    * String "1842 – Treaty 5 March 1856)[5]" should consists of following words: 1842,Treaty,5,March,1856,5
   *
   * @param line any string
    * @return
    */
  def words(line: String): Array[Word] = {
    // TODO Task #3: Tokenize (split string into words) 
     line.replaceAll("[^a-zA-Z0-9^]"," ")
      .split (" ")
      .filter(x=>x.nonEmpty)
  }

  def tokenize(rdd: RDD[String]): RDD[Word] = rdd.flatMap(words)

  /**
    * Task #4: Count words in RDD
    * Given RDD[String]. You need tokenize it using method words() and count words
    * @param rdd input RDD
    * @return word count
    */
  def countWords(rdd: RDD[Word]): Amount = {
    // TODO Task #4: Count words in RDD
    val wordRDD = tokenize(rdd)
    wordRDD.count()
  }

  /**
    * Task #7: Transform RDD so that it should contain numbers only
    * i.e. string "1842 – Treaty 5 March 1856)[5]" should consists of following numbers:
    * 1842, 5, 1856, 5
    *
    * @param text RDD with text
    * @return RDD with numbers only
    */

  def numbers(text: RDD[Word]): RDD[Long] = {
    // TODO Task #7: Transform RDD so that it should contain numbers only
    val num = tokenize(text)
    num.map(_.replaceAll("[^0-9]+", ""))
      .filter(_.nonEmpty)
      .map(_.toLong)
  }

  /**
    * Task #10: Get word occurrences
    * Count how often each word repeats
    *
    * @return
    */
  def wordFrequency(words: RDD[Word]):RDD[(Word, Int)] = {
    // TODO Task #10: Get word occurrences
    // TODO Task #10.1: Replace output type RDD[Any] with correct one

    var wordF = tokenize(words)
      .map(word => (word,1))
      .reduceByKey(_+_)
    wordF
  }

  /**
    * Task #12a: Gather word stats by 4 criteria such as:
    *   A = Number of digits
    *   B = Number of vowels
    *   C = Number of consonants
    *   D = Number of other symbols
    *
    * @param word word need to be classified
    * @return
    */
  def wordStats(word: Word): WordStats = {
    // TODO Task #12a: Gather word stats by 4 criteria
    val a = word.replaceAll("[^0-9]+", "")
      .split("")
      .filter(_.nonEmpty)
      .length
    val b = word.replaceAll("[^aeiouyAEIOUY]+", "")
      .split("")
      .filter(_.nonEmpty)
      .length
    val c = word.replaceAll("[aeiouyAEIOUY]+", "")
      .replaceAll("[^a-zA-Z]+","")
      .split("")
      .filter(_.nonEmpty)
      .length
    val d = word.replaceAll("[a-zA-Z0-9]+", "")
      .split("")
      .filter(_.nonEmpty)
      .length
    (a,b,c,d)
  }

  /**
    * Task #12b: Classify word statistics into 5 groups such as:
    *   Group 0: where D > 0 or A > 0 and B+C >0, name it “thrash”
    *   Group 1: where A > 0, name it “numbers”
    *   Group 2: where B == C, name it “balanced_words”
    *   Group 3: where B > C, name it “singing_words”
    *   Group 4: others, name it “grunting_words”
    * Where:
    *   A = Number of digits
    *   B = Number of vowels
    *   C = Number of consonants
    *   D = Number of other symbols
    *
    * @param wordStats word statistics (A, B, C, D)
    * @return
    */
  def wordStatsClassifier(wordStats: WordStats): Classifier = {
    // TODO Task #12b: Classify word by
    if(wordStats._4 > 0 || wordStats._1 > 0 && wordStats._2 + wordStats._3 > 0)
      return "thrash"
    else if(wordStats._1 > 0)
      return "numbers"
    else if(wordStats._2 == wordStats._3)
      return "balanced_words"
    else if(wordStats._2 > wordStats._3)
      return "singing_words"
    else
      return "grunting_words"
  }

  def wordClassifier(word: Word): Classifier = {
    wordStatsClassifier(wordStats(word))
  }

  /**
    * Task #13a: How many elements there are in each group
    * Hint: Use wordClassifier() to implement this method
    *
    * @param words words for classification
    * @return classification
    */
  def classify(words: RDD[Word]): Map[Classifier, Amount] = {
    // TODO Task #13a: How many elements there are in each group
    val countElem = words.map(word => (wordClassifier(word),1.toLong))
      .reduceByKey(_+_)
    var m: Map[Classifier, Amount] = Map[Classifier, Amount]()
    for(value <- countElem.collect){
      m += value._1 -> value._2
    }
    m
  }

}
