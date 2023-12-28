package org.example

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object Task2 {

  def task2BruteForce(data: RDD[List[Double]], top: Int) = {
    data.cartesian(data)
      .filter(pair => pair._1 != pair._2)
      .groupByKey()
      .map(pair => (pair._1, countDominatedPoints(pair._1, pair._2)))
      .sortBy(_._2) //sort map by value in descending order
      .top(top)(Ordering[Long].on(_._2))
  }

  def countDominatedPoints(key: List[Double], values: Iterable[List[Double]]): Long = {
    var totalPoints = 0
    values
      .filter(p => !p.equals(key))
      .foreach(nums => {
        if(Task1.dominates(key, nums)) totalPoints += 1
      })
    totalPoints
  }

  def STD2(data: RDD[List[Double]], top: Int,  sc: SparkContext) = {
    if(top == 0) Array()

    val skylinePoints = sc.parallelize(Task1.ALS(data).toList).cartesian(data)
      .filter(pair => pair._1 != pair._2)
      .map(points => Tuple2(points._1, if (Task1.dominates(points._1, points._2)) 1L else 0L))
      .reduceByKey(_+_) //for every skyline point calculate its dominance score
      .sortBy(-_._2) //sort them based on their dominance score
      .collect()
      .to[mutable.ArrayBuffer]

    val point = skylinePoints.head
    val result = Array(point) //add the point with the max dominance score to the result array
    skylinePoints.remove(0)

    val answer = calculatePoints2(top - 1, point._1, data.filter(p => !p.equals(point)), result, skylinePoints)

    answer
  }

  def calculatePoints2(topK: Int, point: List[Double], data: RDD[List[Double]], result: Array[(List[Double], Long)],
                       toCalculatePoints: ArrayBuffer[(List[Double], Long)]): Array[(List[Double], Long)] = {
    if(topK == 0)  return result

    var res = result

    data
      .filter(p => !p.equals(point)) //filter the current point
      .filter(p => !toCalculatePoints.map(_._1).contains(p)) //filter the point if it's already in toCalculatePoints array
      .filter(p => Task1.dominates(point, p)) //get only the points that are dominated by 'point'
      .filter(p => isInRegion2(point, p, toCalculatePoints)) //get only the points belonging to region of current point
      .cartesian(data)
      .map(points => Tuple2(points._1, if (Task1.dominates(points._1, points._2)) 1L else 0L))
      .reduceByKey(_+_) //for every skyline point calculate its dominance score
      .sortBy(-_._2) //sort them based on their dominance score
      .collect()
      .foreach(p => toCalculatePoints.append(p))

    //Add points belonging only to region of point to the toCalculatePoints RDD
    val newCalculatePoints = toCalculatePoints
      .sortWith(_._2 > _._2) //sort points based on their dominance score

    val pointToAdd = toCalculatePoints.apply(0)
    res :+= pointToAdd //add first point(the one with the maximum score) to the result array

    newCalculatePoints.remove(0)
    calculatePoints2(topK - 1, pointToAdd._1, data.filter(p => !p.equals(pointToAdd)), res, newCalculatePoints)

  }

  //TODO remove sc if not needed
  def STD(data: RDD[List[Double]], top: Int,  sc: SparkContext) = {
    if(top == 0) Array()

    val skylinePoints = Task1.ALS(data).to[mutable.ArrayBuffer] //find skyline points
      .map(point => Tuple2(point, countDominatedPoints2(point, data.collect()))) //for every skyline point calculate its dominance score
      .sortWith(_._2 > _._2) //sort them based on their dominance score

    val point = skylinePoints.apply(0)
    val result = Array(point) //add the point with the max dominance score to the result array
    skylinePoints.remove(0)

    val answer = calculatePoints(top - 1, point._1, data.filter(p => !p.equals(point)).collect(), result, skylinePoints)

    answer
  }

  //TODO rename toCalculatePoints
  def calculatePoints(topK: Int, point: List[Double], data: Array[List[Double]], result: Array[(List[Double], Long)],
                      toCalculatePoints: ArrayBuffer[(List[Double], Long)]): Array[(List[Double], Long)] = {
    if(topK == 0)  return result

    var res = result

    data
      .filter(p => !p.equals(point)) //filter the current point
      .filter(p => !toCalculatePoints.map(_._1).contains(p)) //filter the point if it's already in toCalculatePoints array
      .filter(p => Task1.dominates(point, p)) //get only the points that are dominated by 'point'
      .filter(p => isInRegion2(point, p, toCalculatePoints)) //get only the points belonging to region of current point
      .map(p => Tuple2(p, countDominatedPoints2(p, data))) //count the dominated points for each
      .foreach(p => toCalculatePoints.append(p)) //add every point toCalculatePoints array

    //Add points belonging only to region of point to the toCalculatePoints RDD
    val newCalculatePoints = toCalculatePoints
      .sortWith(_._2 > _._2) //sort points based on their dominance score

    val pointToAdd = toCalculatePoints.apply(0)
    res :+= pointToAdd //add first point(the one with the maximum score) to the result array

    newCalculatePoints.remove(0)
    calculatePoints(topK - 1, pointToAdd._1, data.filter(p => !p.equals(pointToAdd)), res, newCalculatePoints)

  }

  //Finds if pointB is in the region of pointA
  def isInRegion2(pointA: List[Double], pointB: List[Double], skylines: ArrayBuffer[(List[Double], Long)]): Boolean = {
    skylines
      .map(_._1) //get the points, without their dominance score
      .filter(p => !p.equals(pointA))  //get the points that are not equal to pointA
      .map(point => !Task1.dominates(point, pointB)) //map to false or true depending if pointB is not dominated by point
      .reduce(_&&_)
  }

  def countDominatedPoints3(point: List[Double], points: Iterable[List[Double]]): Long = {
    points
      .filter(p => !p.equals(point))
      .count(p => Task1.dominates(point, p))
  }

  def countDominatedPoints2(point: List[Double], points: Array[List[Double]]) : Long = {
    points
      .filter(p => !p.equals(point))
      .count(p => Task1.dominates(point, p))
  }

}
