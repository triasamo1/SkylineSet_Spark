package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io._
import scala.collection.mutable.ArrayBuffer

object Spark {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First App")
    println("hello")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext( conf)

    val input = sc.textFile("data3.txt")

    val parsedData = input
      .map(s => s.split(" ")
        .filter(_.nonEmpty)
        .map(_.toDouble)
        .toList)

    val dimensions = parsedData.first().length
    val totalPoints = parsedData.count()


    // ----------- Task 1 ----------
    RunHelper.runTask(dimensionsTotalPointsText(dimensions, totalPoints), () => Task1.sfs(parsedData), 11)
    RunHelper.runTask3(dimensionsTotalPointsText(dimensions, totalPoints), () => Task1.ALS(parsedData), 1)

    // ----------- Task 2 ----------
    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints), () => Task2.STDWithRec2(parsedData, 3, sc), 22)
    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints), () => Task2.STDWithRec(parsedData, 3, sc), 22)
    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints), () => Task2.topPoints(parsedData, 3, sc), 2)

    // ----------- Task 3 ----------
    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints), () => Task3.topKSkylineBruteForce(parsedData, 3), 333)

    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints), () => Task3.task3(parsedData, 3, sc), 333)
    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints), () => Task3.task33(parsedData, 3, sc), 33)
    RunHelper.runTask2(dimensionsTotalPointsText(dimensions, totalPoints), () => Task3.topKSkylinePoints(parsedData, 3, sc), 3)
  }

  def dimensionsTotalPointsText(dim: Int, totalPoints: Long): String = {
    "\nDimensions = " + dim +
      "\nTotal Points = " + totalPoints + "\n"
  }
}
