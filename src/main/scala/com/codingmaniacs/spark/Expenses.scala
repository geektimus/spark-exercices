package com.codingmaniacs.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Expenses {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Expenses")

    // Load each line of the source data into an RDD
    val lines = sc.textFile("../data/customer-orders.csv")

    // Use our parseLines function to convert to (age, numFriends) tuples
    val result = lines
      .map(extractCustomerPricePairs)
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)
      .collect()

    result.foreach(value => printf("Customer: %03d,\tAmount: %1.2f\n", value._1, value._2))
  }

  val extractCustomerPricePairs: String => (Int, Float) = (line: String) => {
    val parts = line.split(",")
    (parts(0).toInt, parts(2).toFloat)
  }
}
