package org.example

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object Task1 {

  def distanceFromStart(point: List[Double]) = {
    var sum = 0.0
    point.foreach(value => {
      sum += (value - 0).abs
    })

    sum
  }

  def sfs(data: RDD[List[Double]]) = {
    //    val rdd2: RDD[List[Double]] = datasetRDD.repartition(num_of_partitions).mapPartitions(addScoreAndCalculate)
    //First sort points based on distance from 0,0,0,0
    val dataList = data
      .map(point => Tuple2(point, distanceFromStart(point)))
      .sortBy(_._2)
      .collect()

    val skyline = ArrayBuffer[List[Double]]()

    skyline +=  dataList.apply(0)._1

    for(i <- 1 until dataList.length) {
      val p1 = dataList.apply(i)._1
      var toBeAdded = true
      var j = 0
      breakable {
        while(j < skyline.length) {
          val p2 = skyline.apply(j)
          if(dominates(p1, p2)) {
            skyline.remove(j)
            j -= 1
          } else if (dominates(p2, p1)) {
            toBeAdded = false
            break()

          }
          j += 1
        }
        if(toBeAdded) skyline += p1
      }
    }
    skyline

  }

  def task1BruteForce(data: RDD[List[Double]]) = {
    val answer = data.cartesian(data)
      .filter(pair => pair._1 != pair._2)
      .groupByKey()
      .filter(pair => isSkyline(pair._1, pair._2))
      .map(pair => pair._1)

    answer
  }

  def isSkyline(key: List[Double], values: RDD[List[Double]]): Boolean = {
    values.foreach(nums => {
      if(isDominated(key, nums)) return false
    })
    true
  }

  // Checks if pointA dominates pointB
  def dominates(pointA: List[Double], pointB: List[Double]): Boolean = {
    var dominates = false
    breakable{
      for (i <- pointA.indices) {
        val p1 = pointA.apply(i)
        val p2 = pointB.apply(i)
        if (p1 > p2) {
          dominates = false
          break()
        }
        if(p1 < p2) dominates = true
      }
    }

    dominates
  }

  def isSkyline(key: List[Double], values: Iterable[List[Double]]): Boolean = {
    values.foreach(nums => {
      if(isDominated(key, nums)) return false
    })
    true
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
