package org.example

import org.apache.spark.rdd.RDD

object Task1 {

  def blockNestedLoops(data: RDD[List[Double]]) = {

  }

  def task1BruteForce(data: RDD[List[Double]]) = {
//    val data2 = data.collect()
//    val temp = data
//      .filter(point1 => isSkyline(point1, data2.filter(point2 => point1 != point2)))
//
//    temp


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
