package org.example

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.asJavaIterableConverter

object FileHelper {

  def createDir(path: String): Boolean = {
    // Create directories if they don't exist
    new File(path).mkdirs()
  }

  def getWriter(path: String): PrintWriter = {
    new PrintWriter(new File("results/" + path))
  }

  def getStatsText(text: String, taskNumber: Int, duration: Long, resultPoints: Long): String = {
    "--Solution File for Task " + taskNumber + " --\n" +
      "Total time to run = " + duration + "ms" +
      text +
      getTextBasedOnTask(taskNumber, resultPoints) + //TODO
      "---------------------------------------------------------\n"
  }

  def getTextBasedOnTask(task: Int, resultPoints: Long): String = {
    if (task == 1) "Total skyline points found =  " + resultPoints + "\n"  else "\n"
  }

  def writeToPerformanceFile(algorithm: String, duration: Long, distribution: String) = {
    // File path
    val csvFilePath: String = "performance.csv"

    // Data to be appended
    val newData: Seq[String] = Seq(distribution, algorithm, duration.toString)

    // Check if the file exists
    val fileExists: Boolean = Files.exists(Paths.get(csvFilePath))

    // Append data to the CSV file
    val csvLines: Seq[String] = if (fileExists) {
      Seq(newData.mkString(","))
    } else {
      Seq("Distribution,Algorithm,Duration", newData.mkString(","))
    }

    Files.write(Paths.get(csvFilePath), csvLines.asJava, StandardOpenOption.CREATE, StandardOpenOption.APPEND)

    println(s"Data has been appended to '$csvFilePath'.")
  }

  def writeToFile(text: String, taskNumber: Int, duration: Long, answer: List[List[Double]], filePath: String): Unit = {
    val writer = getWriter(filePath)

    writer.write(getStatsText(text, taskNumber, duration, answer.size.toLong))
    answer
      .map(p => p.map(_.toString).mkString(", ") + "\n")
      .foreach(p => writer.write(p))
    writer.close()
  }

  def writeToFile(text: String, taskNumber: Int, duration: Long, answer: Array[(List[Double], Long)], filePath: String): Unit = {
    val writer = getWriter(filePath)

    writer.write(getStatsText(text, taskNumber, duration, answer.length.toLong))

    answer
      .map(_._1)  //TODD for 2
      .map(p => p.map(_.toString).mkString(", ") + "\n")
      .foreach(p => writer.write(p))
    writer.close()
  }

  def writeToFile2(text: String, taskNumber: Int, duration: Long, answer: List[(List[Double], Long)], filePath: String): Unit = {
    val writer = getWriter(filePath)

    writer.write(getStatsText(text, taskNumber, duration, answer.size.toLong))

    answer
      .map(_._1) //TODD for 2
      .map(p => p.map(_.toString).mkString(", ") + "\n")
      .foreach(p => writer.write(p))
    writer.close()
  }

  def writeToFile2(text: String, taskNumber: Int, duration: Long, answer: ArrayBuffer[(List[Double], Long)], filePath: String): Unit = {
    val writer = getWriter(filePath)

    writer.write(getStatsText(text, taskNumber, duration, answer.size.toLong))

    answer
      .map(_._1) //TODD for 2
      .map(p => p.map(_.toString).mkString(", ") + "\n")
      .foreach(p => writer.write(p))
    writer.close()
  }
}
