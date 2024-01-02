package org.example

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object Task3 {

  def topKSkylineBruteForce(data: RDD[List[Double]], top: Int) = {
    Task1.task1BruteForce(data)
      .collect()
      .map(point => Tuple2(point, Task2.countDominatedPoints2(point, data.collect())))
      .sortBy(-_._2)
      .take(top)
  }

  def topKSkylinePoints(data: RDD[List[Double]], top: Int, sc: SparkContext): Array[(List[Double], Long)] = {
    val broadcastData = sc.broadcast(data.collect())
    val skylines = Task1.ALS(data).toList //find skyline points

    sc.parallelize(skylines)
      .mapPartitions(par => par
        .map(point => (point, countDominatedPoints(point, broadcastData))))
      .sortBy(-_._2)
      .take(top)
  }

  def task3(data: RDD[List[Double]], top: Int, sc: SparkContext): Array[(List[Double], Long)] = {
    sc.parallelize(Task1.sfs(data)).cartesian(data)
      .filter(pair => pair._1 != pair._2)
      .map(pair => (pair._1, if (Task1.dominates(pair._1, pair._2)) 1L else 0L))
      .reduceByKey(_+_)
      .sortBy(-_._2)
      .take(top)
  }

  def task33(data: RDD[List[Double]], top: Int, sc: SparkContext): Array[(List[Double], Long)] = {

    sc.parallelize(Task1.ALS(data).toList).cartesian(data)
      .filter(pair => pair._1 != pair._2)
      .groupByKey()
      .map(point => Tuple2(point._1, Task2.countDominatedPoints3(point._1, point._2)))
      .sortBy(-_._2)
      .take(top)
  }

  def countDominatedPoints(point: List[Double], values: Broadcast[Array[List[Double]]]): Long = {
    var totalPoints = 0
    values
      .value
      .filter(p => !p.equals(point))
      .foreach(nums => {
        if(Task1.dominates(point, nums)) totalPoints += 1
      })
    totalPoints
  }
}
