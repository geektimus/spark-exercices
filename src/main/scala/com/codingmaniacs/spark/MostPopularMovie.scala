package com.codingmaniacs.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object MostPopularMovie {
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "MostPopularMovie")

    time {
      withJoin(sc)
    }

    time {
      withBroadcast(sc)
    }
  }


  def withBroadcast(sc: SparkContext): Unit = {
    // Load Movie Names
    val movieData = sc.broadcast(loadMovieNames())

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../ml-100k/u.data")

    val movieIDs = lines
      .map(line => line.split("\t")(1).toInt)

    val popularMovies = movieIDs
      .map((_, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)
      .map(partialMovieData => (movieData.value(partialMovieData._1), partialMovieData._2))
      .take(10)

    popularMovies.foreach(movie => println(s"{movieId: ${movie._1}, rating: ${movie._2}}"))
  }

  def withJoin(sc: SparkContext): Unit = {
    val extraData = sc.textFile("../ml-100k/u.item")

    val movieData = extraData
      .map(line => {
        val parts = line.split("\\|")
        (parts(0).toInt, parts(1))
      })

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../ml-100k/u.data")

    val movieIDs = lines
      .map(line => line.split("\t")(1).toInt)

    val popularMovies = movieIDs
      .map((_, 1))
      .reduceByKey((x, y) => x + y)


    val result = popularMovies
      .join(movieData)
      .map(values => (values._2._2, values._2._1))
      .sortBy(_._2, ascending = false)
      .take(10)

    result.foreach(movie => println(s"{movieId: ${movie._1}, rating: ${movie._2}}"))


  }

  /**
    * Load the movie enrichment info on a map mapping movie id to movie name.
    *
    * @return Map(MovieID->MovieName)
    */
  def loadMovieNames(): Map[Int, String] = {
    // Handling encoding

    implicit val codec: Codec = Codec(Codec.UTF8.charSet)
    codec
      .onMalformedInput(CodingErrorAction.REPLACE)
      .onUnmappableCharacter(CodingErrorAction.REPLACE)

    val lines = Source.fromFile("../ml-100k/u.item").getLines()

    lines
      .map(line => {
        val parts = line.split("\\|")
        (parts(0).toInt, parts(1))
      })
      .toMap
  }

  /**
    * Measures the time it takes to execute the given block of code
    *
    * @param block block of code
    */
  def time[R](block: => Unit): Unit = {
    val start = System.nanoTime()
    block
    val end = System.nanoTime()
    println("Elapsed time: " + (end - start) / 1000000 + "ms")
  }
}
