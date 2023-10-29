package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark {


  def main(args: Array[String]): Unit = {

    var conf=new SparkConf()
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


    val answe = pairs
      .filter(pair => this.isSkyline2(pair._1, pair._2))
      .map(pair => pair._1)

    answe.collect.foreach(arr => println(arr))

//    parsedData.collect.foreach(arr => println(arr))

//    parsedData.collect.foreach(arr => println(arr))
//    findSkylinePoints(parsedData).foreach(arr => println(arr))
  }

  def isSkyline(key: List[Double], values: Iterable[List[Double]]): Boolean = {

    values.foreach(value => {
      if(!isNotDominated2(key, value)) return false
    })

    true
  }

  def testFunc(d: Double, doubles: Iterable[Double]) = {
    var min = true
    doubles.foreach(num => {
      if(num <= d) min = false
    })
    min
  }

  def isSkyline2(key: List[Double], values: Iterable[List[Double]]): Boolean = {
    for( i <- key.indices) {
      if(testFunc(key.apply(i), values.map(x => x.apply(i)))) return true
    }
    false
  }

  def findSkylinePoints(list: RDD[List[Double]]) : List[Double] = {
    var answer = Array[Double]()
    list.foreach{i =>

      list.foreach{j=>
        if (isNotDominated(i, j)) {
          answer :+ i
        }
      }
    }
    answer.toList
  }

  def isNotDominated(pointA: List[Double], pointB: List[Double]) : Boolean = {
    var answer = Array[Boolean]()

    pointA.foreach(i => {
      var max = true
      pointB.foreach(j => {
        if(j >= i) max = false
      })
      if(!max){
        answer :+= false
      } else {
        answer :+= true
      }
    })

    if (answer.distinct.length == 2) true else false
  }

  def isNotDominated2(pointA: List[Double], pointB: List[Double]) : Boolean = {
    var answer = Array[Boolean]()

    for( i <- pointA.indices)
    {
      answer :+= isBetter(pointA.apply(i), pointB.apply(i))
    }

    if (answer.distinct.length == 2) true else false
  }

  def isBetter(num1: Double, num2: Double): Boolean = {
    if(num2 >= num1) false else true
  }

}
