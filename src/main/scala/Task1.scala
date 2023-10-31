package org.example

import org.apache.spark.rdd.RDD

object Task1 {

  def task1BruteForce(pairs: RDD[(List[Double], Iterable[List[Double]])]) = {
    val start = System.currentTimeMillis()
    val answer = pairs
      .filter(pair => this.isSkyline(pair._1, pair._2))
      .map(pair => pair._1)

    val end = System.currentTimeMillis()

    println("-- Task 1 --")
    println("Total time = " + (end - start) + "ms")
    println("Total skyline points = " + answer.count())
    answer.collect.foreach(arr => println(arr))
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
