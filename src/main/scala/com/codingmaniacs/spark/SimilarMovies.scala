package com.codingmaniacs.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}


object SimilarMovies {

  // Types to handle the movie data easily
  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  val scoreThreshold = 0.97
  val coOccurrenceThreshold = 50.0

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RecommendedMovies")

    // Load up each line of the ratings data into an RDD
    val data = sc.textFile("../ml-100k/u.data")

    val userRatingsRDD = data
      .map(_.split("\t"))
      .map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

    // Self Join userId => (movieID1, rating2), (movieID2, rating2)
    val joinedRatings = userRatingsRDD.join(userRatingsRDD)

    val uniqueRatings = joinedRatings
      .filter(filterDuplicates)
      .map(userRating => ((userRating._2._1._1, userRating._2._2._1), (userRating._2._1._2, userRating._2._2._2)))
      .groupByKey()

    val moviePairSimilarities = uniqueRatings.mapValues(computeCosineSimilarity).cache()

    if (args.length > 0) {

      val movieID: Int = args(0).toInt

      val filterGoodMovies = filterGoodMoviesById(movieID)
      val filteredResults = moviePairSimilarities.filter(filterGoodMovies)

      // Sort by quality score.
      val results = filteredResults.map(x => (x._2, x._1)).sortByKey(ascending = false).take(10)
      val nameDict = loadMovieNames()

      println("\nTop 10 similar movies for " + nameDict(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }
  }

  private val filterGoodMoviesById = (movieID: Int) => (data: ((Int, Int), (Double, Int))) => {
    val pair = data._1
    val sim = data._2
    (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurrenceThreshold
  }

  val filterDuplicates: UserRatingPair => Boolean = (userRatingPair: UserRatingPair) => {
    val rating1 = userRatingPair._2._1
    val rating2 = userRatingPair._2._2

    val movie1 = rating1._1
    val movie2 = rating2._1

    movie1 < movie2
  }

  val computeCosineSimilarity: RatingPairs => (Double, Int) = (ratingPairs: RatingPairs) => {
    var numPairs = 0
    var sum_xx = 0.0
    var sum_yy = 0.0
    var sum_xy = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator = sum_xy
    val denominator = Math.sqrt(sum_xx) * Math.sqrt(sum_yy)

    var score = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    (score, numPairs)
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

}
