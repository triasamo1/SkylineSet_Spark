package org.example

import scala.collection.mutable.ArrayBuffer

object RunHelper {
  def runTask2(text: String, function: () => Array[(List[Double], Long)], taskNumber: Int): Unit = {
    val start = System.currentTimeMillis()

    val answer = function.apply()

    val end = System.currentTimeMillis()

    println("-- " + taskNumber + " --")
    println("Total time = " + (end - start) + "ms")
    println("Total skyline points = " + answer.length)

    FileHelper.writeToFile(text, taskNumber, end - start, answer, "task" + taskNumber + "Results.txt")

    answer.foreach(arr => println(arr))
  }

  def runTask3(text: String, function: () => Iterator[List[Double]], taskNumber: Int): Unit = {
    val start = System.currentTimeMillis()

    val answer = function.apply().toList

    val end = System.currentTimeMillis()

    println("-- " + taskNumber + " --")
    println("Total time = " + (end - start) + "ms")
    println("Total skyline points = " + answer.length)

    FileHelper.writeToFile(text, taskNumber, end - start, answer, "task" + taskNumber + "Results.txt")
    answer.foreach(arr => println(arr))
  }

  def runTask5(text: String, function: () => Iterator[(List[Double], Long)], taskNumber: Int): Unit = {
    val start = System.currentTimeMillis()

    val answer = function.apply().toList

    val end = System.currentTimeMillis()


    println("-- " + taskNumber + " --")
    println("Total time = " + (end - start) + "ms")
    println("Total skyline points = " + answer.length)

    FileHelper.writeToFile2(text, taskNumber, end - start, answer, "task" + taskNumber + "Results.txt")

    answer.foreach(arr => println(arr))
  }


  def runTask(text: String, function: () => ArrayBuffer[List[Double]], taskNumber: Int): Unit = {
    val start = System.currentTimeMillis()
    val answer = function.apply()
    val end = System.currentTimeMillis()

    println("-- " + taskNumber + " --")
    println("Total time = " + (end - start) + "ms")
    println("Total skyline points = " + answer.length)

    FileHelper.writeToFile(text, taskNumber, end - start, answer.toList, "task" + taskNumber + "Results.txt")
    answer.foreach(arr => println(arr))
  }

  //  def runTask2[T <: RDD[List[Double]] with Iterator[List[Double]]](taskNumber: Int, pointsFunction: () => T, sizeFunction: T => Long): Unit = {
  //    val start = System.currentTimeMillis()
  //
  //    val answer = pointsFunction.apply()
  //    val end = System.currentTimeMillis()
  //
  //    println("-- " + taskNumber + " --")
  //    println("Total time = " + (end - start) + "ms")
  //    println("Total skyline points = " + sizeFunction.apply(answer))
  //
  //    FileHelper.writeToFile(() => "--Solution File for Task " + taskNumber + " --\n" +
  //      "Total time to run = " + (end - start) + "ms" +
  //      getTextBasedOnTask(taskNumber) +
  //      "---------------------------------------------------------\n", answer, "task" + taskNumber + "Results.txt")
  //
  //    answer.foreach(arr => println(arr))
  //  }
}
