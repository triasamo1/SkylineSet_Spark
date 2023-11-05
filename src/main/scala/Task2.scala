package org.example

import org.apache.spark.rdd.RDD


object Task2 {

  def task2BruteForce(data: RDD[List[Double]], top: Int) = {
    val answer = data.cartesian(data)
      .filter(pair => pair._1 != pair._2)
      .groupByKey()
      .map(pair => (pair._1, countDominatedPoints(pair._1, pair._2)))
      .sortBy(_._2) //sort map by value in descending order
      .top(top)(Ordering[Long].on(_._2))

    answer
  }

  def countDominatedPoints(key: List[Double], values: Iterable[List[Double]]): Long = {
    var totalPoints = 0
    values.foreach(nums => {
      if(!Task1.isDominated(key, nums)) totalPoints += 1
    })
    totalPoints
  }

}
