package com.codingmaniacs.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

/**
  * Finds the degrees of separation between two Marvel comic book characters, based on co-appearances in a comic.
  */
object DegreesOfSeparation {

  // The characters we want to find the separation between.
  val startCharacterID = 5306 //SpiderMan
  val targetCharacterID = 14 //ADAM 3,031 (who?)

  // We make our accumulator a "global" Option so we can reference it in a mapper later.
  var hitCounter: Option[LongAccumulator] = None

  // Some custom data types 
  // BFSData contains an array of hero ID connections, the distance, and color.
  type BFSData = (Array[Int], Int, String)
  // A BFSNode has a heroID and the BFSData associated with it.
  type BFSNode = (Int, BFSData)

  /** Converts a line of raw input into a BFSNode */
  def convertLineToNode(line: String): BFSNode = {

    // Split up the line into fields
    val fields = line.split("\\s+")

    // Extract this hero ID from the first field
    val heroID = fields.head.toInt

    // Extract subsequent hero ID's into the connections array
    val connections = for (el <- fields.tail) yield el.toInt

    // Default distance and color is 9999 and white
    var color: String = "WHITE"
    var distance: Int = 9999

    // Unless this is the character we're starting from
    if (heroID == startCharacterID) {
      color = "GRAY"
      distance = 0
    }

    (heroID, (connections, distance, color))
  }

  /** Create "iteration 0" of our RDD of BFSNodes */
  def createInitialRDD(sc: SparkContext): RDD[BFSNode] = {
    val lines = sc.textFile("../data/marvel-graph.txt")
    lines.map(convertLineToNode)
  }

  /**
    * Expands a BFSNode into this node and its children
    *
    * @param node Node being processed
    * @return Array of nodes
    */
  def bfsMap(node: BFSNode): Array[BFSNode] = {
    node match {
      case (characterID, (connections, distance, color)) =>
        if (color.equals("GRAY")) {
          val processedNodes = connections.map(connection => {
            // Have we stumbled across the character we're looking for?
            // If so increment our accumulator so the driver script knows.
            if (targetCharacterID == connection) {
              if (hitCounter.isDefined) {
                hitCounter.get.add(1)
              }
            }
            (connection, (Array[Int](), distance + 1, color))
          })
          processedNodes :+ (characterID, (connections, distance, "BLACK"))
        } else {
          Array((characterID, (connections, distance, color)))
        }
      case _ => null
    }
  }

  /**
    * Combine nodes for the same heroID, preserving the shortest length and darkest color.
    *
    * @param data1 Hero found following one path
    * @param data2 Hero found following other path
    * @return
    */
  def bfsReduce(data1: BFSData, data2: BFSData): BFSData = {

    // Extract data that we are combining
    val connections1: Array[Int] = data1._1
    val connections2: Array[Int] = data2._1
    val distance1: Int = data1._2
    val distance2: Int = data2._2
    val color1: String = data1._3
    val color2: String = data2._3

    // Default node values
    val DISTANCE: Int = 9999
    var color: String = "WHITE"

    // See if one is the original node with its connections.
    // If so preserve them.
    val connections = connections1 ++ connections2

    // Preserve minimum distance
    val distance = Math.min(Math.min(distance1, distance2), DISTANCE)

    // Preserve darkest color
    if (color1 == "WHITE" && (color2 == "GRAY" || color2 == "BLACK")) {
      color = color2
    }
    if (color1 == "GRAY" && color2 == "BLACK") {
      color = color2
    }
    if (color2 == "WHITE" && (color1 == "GRAY" || color1 == "BLACK")) {
      color = color1
    }
    if (color2 == "GRAY" && color1 == "BLACK") {
      color = color1
    }

    (connections, distance, color)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "DegreesOfSeparation")

    // Our accumulator, used to signal when we find the target
    // character in our BFS traversal.
    hitCounter = Some(sc.longAccumulator("Hit Counter"))

    var iterationRDD = createInitialRDD(sc)

    for (iteration <- 1 to 10) {
      println("Running BFS Iteration# " + iteration)

      // Create new vertices as needed to darken or reduce distances in the
      // reduce stage. If we encounter the node we're looking for as a GRAY
      // node, increment our accumulator to signal that we're done.
      val mapped = iterationRDD.flatMap(bfsMap)

      // Note that mapped.count() action here forces the RDD to be evaluated, and
      // that's the only reason our accumulator is actually updated.  
      println("Processing " + mapped.count() + " values.")

      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          println("Hit the target character! From " + hitCount +
            " different direction(s).")
          return
        }
      }

      // Reducer combines data for each character ID, preserving the darkest
      // color and shortest path.      
      iterationRDD = mapped.reduceByKey(bfsReduce)
    }
  }
}