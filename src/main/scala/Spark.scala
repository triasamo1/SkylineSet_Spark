package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First App")
    println("hello")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext( conf)

    val input = sc.textFile("data.txt")

    val parsedData = input
      .map(s => s.split(" ")
        .filter(_.nonEmpty)
        .map(_.toDouble)
        .toList)

    val pairs = parsedData.cartesian(parsedData)
      .filter(pair => pair._1 != pair._2)
      .groupByKey()

    // ----------- Task 1 ----------
    task1BruteForce(pairs)

    task1DivideConquer(pairs)
  }

  def task1DivideConquer(pairs: RDD[(List[Double], Iterable[List[Double]])]): Unit = {

  }


  def task1BruteForce(pairs: RDD[(List[Double], Iterable[List[Double]])]) = {
    val answer = pairs
      .filter(pair => this.isSkyline(pair._1, pair._2))
      .map(pair => pair._1)

    answer.collect.foreach(arr => println(arr))
  }

  def isSkyline(key: List[Double], values: Iterable[List[Double]]): Boolean = {
    var isNotDominated = true

    values.foreach(nums => {
      if(isDominated(key, nums)) {
        isNotDominated = false
      }
    })

    isNotDominated
  }

  def isDominated(num1: List[Double], num2: List[Double]): Boolean = {
    var isDominated = 0
    for( i <- num1.indices) {
      if(num2.apply(i) <= num1.apply(i)) {
        isDominated += 1
      }
    }
    if(isDominated == num1.size) {
      return true
    }

    false
  }

}
