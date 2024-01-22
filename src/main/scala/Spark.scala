package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Spark {


  def main(args: Array[String]): Unit = {

    val cores = 8  // Set the number of cores you want to use

    val conf = new SparkConf()
    conf.setMaster(s"local[$cores]")
    conf.setAppName("Skyline App")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext(conf)

    if (args.length == 0) {
      println("You need to provide an input file")
      System.exit(1)
    }

    val input = sc.textFile(args.apply(0))

    val parsedData = input
      .map(s => s.split(" ")
        .filter(_.nonEmpty)
        .map(_.toDouble)
        .toList)

    val dimensions = parsedData.first().length
    val totalPoints = parsedData.count()

    FileHelper.createDir("results")
    val distribution = getDistribution(args.apply(0))

    // ----------- Task 1 ----------
//    RunHelper.runTask4(dimensionsTotalPointsText(dimensions, totalPoints),
//      () => Task1.skylinesBruteForce(parsedData), 1, "BruteForce", distribution)
//    RunHelper.runTask(dimensionsTotalPointsText(dimensions, totalPoints), () => Task1.SFS(parsedData), 1, "SFS", distribution)
//    RunHelper.runTask(dimensionsTotalPointsText(dimensions, totalPoints), () => Task1.SALSA(parsedData), 1, "SALSA", distribution)
//    RunHelper.runTask3(dimensionsTotalPointsText(dimensions, totalPoints), () => Task1.ALS(parsedData), 1, "ALS", distribution)

    // ----------- Task 2 ----------
//    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints),
//      () => Task2.task2BruteForce(parsedData, 3), 2, "BruteForce", distribution)
//    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints),
//      () => Task2.topKDominating(parsedData, 3, new ArrayBuffer[(List[Double], Long)]()), 2, "STDRecursive", distribution)
//    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints),
//      () => Task2.STD(parsedData, 3), 2, "STD", distribution)
//    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints),
//      () => Task2.topKDominatingPoints(parsedData, 3, new ArrayBuffer[(List[Double], Long)](), sc), 2, "STDPartitions", distribution)
//    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints),
//      () => Task2.Top_k_GridDominance(parsedData, dimensions, 3, sc), 2, "Grid", distribution)
//
//
//    // ----------- Task 3 ----------
//    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints),
//      () => Task3.topKSkylineBruteForce(parsedData, 3), 3, "BruteForce", distribution)
    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task3.topKSkylinePoints(parsedData, 3), 3, "fromPoints", distribution)
    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints),
      () => Task3.topKSkylinePoints2(parsedData, 3, sc), 3, "fromSkyline", distribution)

//    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints),
//      () => Task3.Top_k_GridDominance(parsedData, dimensions, 3, sc), 3, "Grid", distribution)
  }

  def dimensionsTotalPointsText(dim: Int, totalPoints: Long): String = {
    "\nPoint's dimensions = " + dim +
      "\nTotal Points = " + totalPoints + "\n"
  }


  def getDistribution(fileName: String): String = {
    fileName match {
      case s if s.contains("uniform") => "Uniform"
      case s if s.contains("anticorrelated") => "Anticorrelated"
      case s if s.contains("correlated") => "Correlated"
      case s if s.contains("normal") => "Normal"
      case _ => ""
    }
  }
}