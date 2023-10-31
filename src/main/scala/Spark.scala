package org.example

import org.apache.log4j.{Level, Logger}
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

    val input = sc.textFile("data2.txt")

    val parsedData = input
      .map(s => s.split(" ")
        .filter(_.nonEmpty)
        .map(_.toDouble)
        .toList)

    val pairs = parsedData.cartesian(parsedData)
      .filter(pair => pair._1 != pair._2)
      .groupByKey()

    // ----------- Task 1 ----------
    Task1.task1BruteForce(pairs)

//    task1DivideConquer(parsedData, parsedData.first().size)
  }

//  def task1DivideConquer(points: RDD[List[Double]], dimensions: Int): RDD[List[Double]] = {
//
//    if(points.count() == 1) return points
//    var pivot = points
//      .map(x => x.head)
//      .mean()
//
//    var p1 = points
//      .filter(x => x.head < pivot)
//
//    var p2 = points
//      .filter(x => x.head >= pivot)
//
//    var s1 = task1DivideConquer(p1, dimensions)
//    var s2 = task1DivideConquer(p2, dimensions)
//
//    return mergeBasic(s1, s2, dimensions)
//  }
//
//  def mergeBasic(s1: RDD[List[Double]], s2: RDD[List[Double]], dimensions: Int): RDD[List[Double]] = {
//
//    if(s1){
//
//    } else if()
//
//    return result
//  }

}
