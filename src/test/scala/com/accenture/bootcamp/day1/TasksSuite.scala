package com.accenture.bootcamp.day1

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TasksSuite extends SparkSuite {
  

  test("Task #1: Create RDD from file `1918NewYearHonours.txt`n") {
    val count = newYearHonours.count()
    assert(count == 17037, "1918NewYearHonours.txt should contain 17037 lines")
  }

  test("Task #2: Create RDD from file `ListOfAustralianTreaties.txt`") {
    val count = australianTreaties.count()
    assert(count == 2177, "ListOfAustralianTreaties.txt should contain 2177 lines")
  }

  test("Task #3: Tokenize (split string into words)") {
    val text = "1842 – Treaty 5 March 1856)[5]"
    val w = List("1842", "Treaty", "5", "March", "1856", "5")
    assert(Tokenizer.words(text).toList == w)
  }

  test("Task #3: Tokenize (split string into words) -- using \\t, \\r, \\n as line space") {
    val text = "Welcome\tto\nAccenture\rLatvia! End"
    val w = List("Welcome", "to", "Accenture", "Latvia", "End")
    assert(Tokenizer.words(text).toList == w)
  }

  test("Task #4: Count words in RDD") {
    val lines1 = sc.parallelize(List(
      "Welcome\tto\nAccenture\rLatvia! End"
    ))
    assert(Tokenizer.countWords(lines1) == 5)
    val lines2 = sc.parallelize(List(
      "1842 – Treaty 5 March 1856)[5]"
    ))
    assert(Tokenizer.countWords(lines2) == 6)
  }

  test("Task #5: How many words are in ListOfAustralianTreaties.txt?") {
    assert(Tokenizer.countWords(Data.australianTreaties) == 54657)
  }

  test("Task #6: How many words are in both .txt files?") {
    assert(task6() == 196949)
  }

  test("Task #7: Transform RDD so that it should contain numbers only") {

    val lines = sc.parallelize(List(
      "1842 – Treaty 5 March 1856)[5]"
    ))

    assert(Tokenizer.numbers(lines).collect().toList == List(1842L, 5L, 1856L, 5L))
  }

  test("Task #8: How many unique numbers are in ListOfAustralianTreaties.txt?") {
    assert(task8() == 1149)
  }

  test("Task #9: Calculate average of all numbers in ListOfAustralianTreaties.txt?") {
    assert(task9().toInt == 966)
  }

  test("Task #10: Get word occurrences") {
    val lines = sc.parallelize(List(
      "1842 – Treaty 5 March treaty 1856)[5]"
    ))
    val result = Tokenizer.wordFrequency(lines.flatMap(Tokenizer.words).map(_.toLowerCase)).collect()
    assert(result.length == 5)
    assert(result.contains("5" -> 2))
    assert(result.contains("1842" -> 1))
    assert(result.contains("1856" -> 1))
    assert(result.contains("treaty" -> 2))
    assert(result.contains("march" -> 1))
  }

  test("Task #11: What are 10 most frequent symbols in ListOfAustralianTreaties.txt?") {
    val expected = List("of", "the", "and", "-", "Government", "Agreement", "to", "between", "Australia", "on")
    assert(task11.toList == expected)
  }

  test("Task #12: Split word into 5 groups") {
    val g1 = "thrash"
    val g2 = "numbers"
    val g3 = "balanced_words"
    val g4 = "singing_words"
    val g5 = "grunting_words"

    val classifier = List(
      "."  -> g1, // D
      ".1" -> g1, // D + A
      ".a" -> g1, // D + B
      ":b" -> g1, // D + C
      "0a" -> g1, // A + B
      "1b" -> g1, // A + C
      "1234567890"  -> g2,  // A
      "mama"        -> g3,  // B == C
      "maolun"      -> g3,  // B == C
      "akiko"       -> g4,  // B > C
      "sayonara"    -> g4,  // B > C
      "nymphs"      -> g5,  // B < C
      "wynds"       -> g5   // B < C
    )

    classifier.foreach{
      case (word, expected) =>
        val actual = Tokenizer.wordClassifier(word)
        val actualUp = Tokenizer.wordClassifier(word.toUpperCase())
        assert(actual == expected, s"Word `$word` is expected to be `$expected` but actual is `$actual`")
        assert(actualUp == expected, s"Word `$word` is expected to be `$expected` but actual is `$actualUp`")
    }

  }

  test("Task #13a: How many elements there are in each group") {
    val g1 = "thrash"
    val g2 = "numbers"
    val g3 = "balanced_words"
    val g4 = "singing_words"
    val g5 = "grunting_words"

    val classifier = List(
      "."  -> g1, // D
      ".1" -> g1, // D + A
      ".a" -> g1, // D + B
      ":b" -> g1, // D + C
      "0a" -> g1, // A + B
      "1b" -> g1, // A + C
      "1234567890"  -> g2,  // A
      "mama"        -> g3,  // B == C
      "maolun"      -> g3,  // B == C
      "akiko"       -> g4,  // B > C
      "sayonara"    -> g4,  // B > C
      "nymphs"      -> g5,  // B < C
      "wynds"       -> g5   // B < C
    )

    val expected: Map[String, Long] = classifier.groupBy(_._2).map(m => m._1 -> m._2.size.toLong)
    val rdd = sc.parallelize(classifier.map(_._1))

    assert(Tokenizer.classify(rdd) == expected)

  }

  test("Task #13: How many elements there are in each group in ListOfAustralianTreaties.txt") {
    val g1 = "thrash"
    val g2 = "numbers"
    val g3 = "balanced_words"
    val g4 = "singing_words"
    val g5 = "grunting_words"

    val expected = Map(
      g1 -> 17,
      g2 -> 8650,
      g3 -> 12899,
      g4 -> 3258,
      g5 -> 29833
    )

    assert(task13 == expected)
  }

  test("Task #14: Print samples of each group with A, B, C, D values from ListOfAustralianTreaties.txt?") {
    val actual = task14.collect()

    assert(actual.length > 0)

    actual.foreach{
      case ("numbers", (num, wov, cons, othr)) =>
        assert(num > 0)
        assert(wov == 0)
        assert(cons == 0)
        assert(othr == 0)
      case ("balanced_words", (num, wov, cons, othr)) =>
        assert(num == 0)
        assert(wov == cons)
        assert(othr == 0)
      case ("singing_words", (num, wov, cons, othr)) =>
        assert(num == 0)
        assert(wov > cons)
        assert(othr == 0)
      case ("grunting_words", (num, wov, cons, othr)) =>
        assert(num == 0)
        assert(wov < cons)
        assert(othr == 0)
      case ("thrash", (num, wov, cons, othr)) =>
        if (othr == 0)
          assert(wov + cons > 0 && num > 0)
    }

  }
  
}
