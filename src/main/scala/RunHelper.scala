package org.example

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object RunHelper {
  def runTask2(text: String, function: () => Array[(List[Double], Long)], taskNumber: Int, algorithm: String, distribution: String): Unit = {
    val start = System.currentTimeMillis()

    val answer = function.apply()

    val end = System.currentTimeMillis()

    println("-- " + taskNumber + " " + algorithm + " --")
    println("Total time = " + (end - start) + "ms")
    println("Total skyline points = " + answer.length)

    FileHelper.writeToFile(text, taskNumber, end - start, answer, "task" + taskNumber + algorithm + "Results.txt")

    answer.foreach(arr => println(arr))
    FileHelper.writeToPerformanceFile(algorithm, end - start, distribution)
  }

  def runTask4(text: String, function: () => RDD[List[Double]], taskNumber: Int, algorithm: String, distribution: String): Unit = {
    val start = System.currentTimeMillis()

    val answer = function.apply()

    val end = System.currentTimeMillis()

    println("-- " + taskNumber + " " + algorithm + " --")
    println("Total time = " + (end - start) + "ms")
    println("Total skyline points = " + answer.count())

    FileHelper.writeToFile(text, taskNumber, end - start, answer.collect().toList, "task" + taskNumber + algorithm + "Results.txt")

    answer.foreach(arr => println(arr))
    FileHelper.writeToPerformanceFile(algorithm, end - start, distribution)
  }

  def runTask3(text: String, function: () => Iterator[List[Double]], taskNumber: Int, algorithm: String, distribution: String) = {
    val start = System.currentTimeMillis()

    val answer = function.apply().toList

    val end = System.currentTimeMillis()

    println("-- " + taskNumber + " " + algorithm + " --")
    println("Total time = " + (end - start) + "ms")
    println("Total skyline points = " + answer.length)

    FileHelper.writeToFile(text, taskNumber, end - start, answer, "task" + taskNumber + algorithm + "Results.txt")
    answer.foreach(arr => println(arr))
    FileHelper.writeToPerformanceFile(algorithm, end - start, distribution)
  }

  def runTask5(text: String, function: () => Iterator[(List[Double], Long)], taskNumber: Int): Long = {
    val start = System.currentTimeMillis()

    val answer = function.apply().toList

    val end = System.currentTimeMillis()


    println("-- " + taskNumber + " --")
    println("Total time = " + (end - start) + "ms")
    println("Total skyline points = " + answer.length)

    FileHelper.writeToFile2(text, taskNumber, end - start, answer, "task" + taskNumber + "Results.txt")

    answer.foreach(arr => println(arr))
    end - start
  }


  def runTask(text: String, function: () => ArrayBuffer[List[Double]], taskNumber: Int, algorithm: String, distribution: String) = {
    val start = System.currentTimeMillis()
    val answer = function.apply()
    val end = System.currentTimeMillis()

    println("-- " + taskNumber + " " + algorithm + " --")
    println("Total time = " + (end - start) + "ms")
    println("Total skyline points = " + answer.length)

    FileHelper.writeToFile(text, taskNumber, end - start, answer.toList, "task" + taskNumber + algorithm + "Results.txt")
    answer.foreach(arr => println(arr))
    FileHelper.writeToPerformanceFile(algorithm, end - start, distribution)
  }
}
