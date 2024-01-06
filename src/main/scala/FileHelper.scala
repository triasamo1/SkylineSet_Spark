package org.example

import java.io.{File, PrintWriter}
import scala.collection.mutable.ArrayBuffer

object FileHelper {

  def createDir(path: String): Boolean = {
    // Create directories if they don't exist
    new File(path).mkdirs()
  }

  def getWriter(path: String): PrintWriter = {
    new PrintWriter(new File("results/" + path))
  }

  def getStatsText(taskNumber: Int, duration: Long): String = {
    "--Solution File for Task " + taskNumber + " --\n" +
      "Total time to run = " + duration + "ms" +
      getTextBasedOnTask(taskNumber) +
      "---------------------------------------------------------\n"
  }

  def getTextBasedOnTask(task: Int): String = {
    if (task == 1) "Total skyline points found =  \n" else "\n"
  }

  def writeToFile(text: String, taskNumber: Int, duration: Long, answer: List[List[Double]], filePath: String): Unit = {
    val writer = getWriter(filePath)

    writer.write(getStatsText(taskNumber, duration))
    writer.write(text)
    answer
      .map(p => p.map(_.toString).mkString(", ") + "\n")
      .foreach(p => writer.write(p))
    writer.close()
  }

  def writeToFile(text: String, taskNumber: Int, duration: Long, answer: Array[(List[Double], Long)], filePath: String): Unit = {
    val writer = getWriter(filePath)

    writer.write(getStatsText(taskNumber, duration))
    writer.write(text)
    answer
      .map(_._1)  //TODD for 2
      .map(p => p.map(_.toString).mkString(", ") + "\n")
      .foreach(p => writer.write(p))
    writer.close()
  }

  def writeToFile2(text: String, taskNumber: Int, duration: Long, answer: List[(List[Double], Long)], filePath: String): Unit = {
    val writer = getWriter(filePath)

    writer.write(getStatsText(taskNumber, duration))
    writer.write(text)
    answer
      .map(_._1) //TODD for 2
      .map(p => p.map(_.toString).mkString(", ") + "\n")
      .foreach(p => writer.write(p))
    writer.close()
  }

  def writeToFile2(text: String, taskNumber: Int, duration: Long, answer: ArrayBuffer[(List[Double], Long)], filePath: String): Unit = {
    val writer = getWriter(filePath)

    writer.write(getStatsText(taskNumber, duration))
    writer.write(text)
    answer
      .map(_._1) //TODD for 2
      .map(p => p.map(_.toString).mkString(", ") + "\n")
      .foreach(p => writer.write(p))
    writer.close()
  }
}
