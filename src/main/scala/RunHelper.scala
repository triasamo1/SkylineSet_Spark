package org.example

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object RunHelper {
  def runTask(taskNum:Int, text: String, function: () => Array[(List[Double], Long)], taskNumber: Int, algorithm: String, distribution: String): Array[(List[Double], Long)] = {
    val start = System.currentTimeMillis()
    val answer = function()
    val end = System.currentTimeMillis()

//    println("-- " + taskNumber + " " + algorithm + " --")
//    println("Total time = " + (end - start) + "ms")
//    println("Total skyline points = " + answer.length)

    FileHelper.writeToFile(text, taskNumber, end - start, answer, "task" + taskNumber + algorithm + "Results.txt")

//    answer.foreach(arr => println(arr))
    FileHelper.writeToPerformanceFile(taskNum, algorithm, end - start, distribution)
    answer
  }

  def runTask(taskNum: Int, text: String, function: () => RDD[List[Double]], taskNumber: Int, algorithm: String, distribution: String): RDD[List[Double]] = {
    val start = System.currentTimeMillis()
    val answer = function()
    val end = System.currentTimeMillis()

//    println("-- " + taskNumber + " " + algorithm + " --")
//    println("Total time = " + (end - start) + "ms")
//    println("Total skyline points = " + answer.count())

    FileHelper.writeToFile(text, taskNumber, end - start, answer.collect().toList, "task" + taskNumber + algorithm + "Results.txt")
//    answer.foreach(arr => println(arr))
    FileHelper.writeToPerformanceFile(taskNum, algorithm, end - start, distribution)
    answer
  }

  def runTask(taskNum: Int, text: String, function: () => Iterator[List[Double]], taskNumber: Int, algorithm: String, distribution: String): List[List[Double]] = {
    val start = System.currentTimeMillis()
    val calculate = function()
    val end = System.currentTimeMillis()

    val answer = calculate.toList

//    println("-- " + taskNumber + " " + algorithm + " --")
//    println("Total time = " + (end - start) + "ms")
//    println("Total skyline points = " + answer.length)

    FileHelper.writeToFile(text, taskNumber, end - start, answer, "task" + taskNumber + algorithm + "Results.txt")
//    answer.foreach(arr => println(arr))
    FileHelper.writeToPerformanceFile(taskNum, algorithm, end - start, distribution)
    answer
  }

  def runTask(taskNum: Int, text: String, function: () => ArrayBuffer[List[Double]], taskNumber: Int, algorithm: String, distribution: String): ArrayBuffer[List[Double]] = {
    val start = System.currentTimeMillis()
    val answer = function()
    val end = System.currentTimeMillis()

//    println("-- " + taskNumber + " " + algorithm + " --")
//    println("Total time = " + (end - start) + "ms")
//    println("Total skyline points = " + answer.length)

    FileHelper.writeToFile(text, taskNumber, end - start, answer.toList, "task" + taskNumber + algorithm + "Results.txt")
//    answer.foreach(arr => println(arr))
    FileHelper.writeToPerformanceFile(taskNum, algorithm, end - start, distribution)
    answer
  }
}
