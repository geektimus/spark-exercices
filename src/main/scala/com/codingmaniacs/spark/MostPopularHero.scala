package com.codingmaniacs.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MostPopularHero {

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MostPopularHero")

    findTopTenHeroes(sc)

  }

  private def findTopTenHeroes(sc: SparkContext): Unit = {
    val marvelNames = sc.textFile("../data/marvel-names.txt")
    val namesRDD = marvelNames.flatMap(parseHeroNames)

    val marvelGraph = sc.textFile("../data/marvel-graph.txt")

    val mostPopularHero = marvelGraph
      .map(countCoOccurrences)
      .reduceByKey(defaultReducer)

    mostPopularHero.join(namesRDD)
      .map(heroComplex => heroComplex._2)
      .sortBy(_._1, ascending = false)
      .take(10)
      .foreach(hero => println(s"Hero: ${hero._2}, Rating: ${hero._1}"))
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
