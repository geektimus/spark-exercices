package com.codingmaniacs.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MostPopularHero {

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MostPopularHero")

    val marvelNames = sc.textFile("../data/marvel-names.txt")
    val namesRDD = marvelNames.flatMap(parseHeroNames)

    val marvelGraph = sc.textFile("../data/marvel-graph.txt")

    val mostPopularHero = marvelGraph
      .map(countCoOccurrences)
      .reduceByKey(defaultReducer)
      .max()(Ordering.by(_._2))

    val heroLookup = namesRDD.lookup(mostPopularHero._1).head

    println(s"The most popular hero is $heroLookup with ${mostPopularHero._2} co-appearances")
  }

  private def parseHeroNames(line: String) = {
    val fields = line.split("\"").map(line => line.trim.replaceAll("\"", ""))
    if (fields.length > 1) {
      Some((fields(0).toInt, fields(1)))
    } else {
      None
    }
  }

  private def countCoOccurrences(line: String) = {
    val split = line.split("\\s+")
    (split.head.toInt, split.tail.length)
  }

  private val defaultReducer: (Int, Int) => Int = (x, y) => x + y
}
