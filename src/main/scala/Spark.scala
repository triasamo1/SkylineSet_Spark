package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Spark {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Skyline App")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext( conf)

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

    // ----------- Task 1 ----------
    RunHelper.runTask(dimensionsTotalPointsText(dimensions, totalPoints), () => Task1.SFS(parsedData), 11)
    RunHelper.runTask3(dimensionsTotalPointsText(dimensions, totalPoints), () => Task1.ALS(parsedData), 1)

    // ----------- Task 2 ----------
    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints), () => Task2.STD(parsedData, 3), 22)
    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints), () => Task2.STD(parsedData, 3, sc), 22)
    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints), () => Task2.topKDominatingPoints(parsedData, 3, sc), 2)


    // ----------- Task 3 ----------
    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints), () => Task3.topKSkylineBruteForce(parsedData, 3), 333)
    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints), () => Task3.topKSkylinePoints2(parsedData, 3, sc), 333)
    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints), () => Task3.topKSkylinePoints(parsedData, 3, sc), 3)
  }

  def dimensionsTotalPointsText(dim: Int, totalPoints: Long): String = {
    "\nPoint's dimensions = " + dim +
      "\nTotal Points = " + totalPoints + "\n"
  }
}
