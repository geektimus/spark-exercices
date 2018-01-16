name := "Spark Exercises"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1" % "provided"

// Configure MainClass and jar name
mainClass in assembly := Some("com.codingmaniacs.spark.RatingsCounter")
assemblyJarName in assembly := "movielens-scala.jar"

// Exclude Scala on the jar file since spark already includes scala.
assembleArtifact in assemblyPackageScala := false