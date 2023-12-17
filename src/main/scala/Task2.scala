package org.example

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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

  //TODO remove sc if not needed
  def STD(data: RDD[List[Double]], top: Int,  sc: SparkContext) = {
    if(top == 0) Array()

    val skylines = Task1.sfs(data) //find skyline points

    val skylinePoints = skylines
      .map(point => Tuple2(point, countDominatedPoints2(point, data.collect()))) //for every skyline point calculate its dominance score
      .sortWith(_._2 > _._2) //sort them based on their dominance score


    val point = skylinePoints.apply(0)
    val result = Array(point) //add the point with the max dominance score to the result array
    skylinePoints.remove(0)

    val answer = helper2(top - 1, point._1, data.filter(p => !p.equals(point)).collect(), result, skylinePoints)

    answer
  }

  //TODO rename toCalculatePoints
  def helper2(topK: Int, point: List[Double], data: Array[List[Double]], result: Array[(List[Double], Long)],
             toCalculatePoints: ArrayBuffer[(List[Double], Long)]): Array[(List[Double], Long)] = {
    if(topK == 0)  return result

    var res = result

    data
      .filter(p => !p.equals(point)) //filter the current point
      .filter(p => !toCalculatePoints.map(_._1).contains(p)) //filter the point if it's already in toCalculatePoints array
      .filter(p => isInRegion2(point, p, toCalculatePoints)) //get only the points belonging to region of current point
      .map(p => Tuple2(p, countDominatedPoints2(p, data))) //count the dominated points for each
      .foreach(p => toCalculatePoints.append(p)) //add every point toCalculatePoints array

    //Add points belonging only to region of point to the toCalculatePoints RDD
    val newCalculatePoints = toCalculatePoints
      .sortWith(_._2 > _._2) //sort points based on their dominance score

    val pointToAdd = toCalculatePoints.apply(0)
    res :+= pointToAdd //add first point(the one with the maximum score) to the result array

    newCalculatePoints.remove(0)
    helper2(topK - 1, pointToAdd._1, data.filter(p => !p.equals(pointToAdd)), res, newCalculatePoints)

  }

  //Finds if pointB is in the region of pointA
  def isInRegion2(pointA: List[Double], pointB: List[Double], skylines: ArrayBuffer[(List[Double], Long)]): Boolean = {
    skylines
      .map(_._1) //get the points, without their dominance score
//      .filter(p => Task1.dominates(pointA, p)) //get only the points that pointA dominates
      .filter(p => !p.equals(pointA))  //get the points that are not equal to pointA
      .map(point => !Task1.dominates(point, pointB)) //map to false or true depending if pointB is not dominated by point
      .reduce(_&&_)
  }


  def countDominatedPoints2(point: List[Double], points: Array[List[Double]]) : Long = {
    points
      .filter(p => !p.equals(point))
      .count(p => Task1.dominates(point, p))
  }

  def countDominatedPoints(point: List[Double], points: RDD[List[Double]]) : Long = {
//    points
//      .map(p => Task1.dominates(point, p))
//      .count()
    var totalPoints = 0
    points
//      .filter(p => !p.equals(point))
      .foreach(nums => {
        if(Task1.dominates(point, nums)) totalPoints += 1
      })
    totalPoints
  }


  //TODO rename toCalculatePoints
  def helper(topK: Int, point: List[Double], data: RDD[List[Double]], result: Array[(List[Double], Long)],
             toCalculatePoints: RDD[(List[Double], Long)]): Any = {
    if(topK == 0)  return result

      //Add points belonging only to region of point to the toCalculatePoints RDD
    val newCalculatePoints = toCalculatePoints.union(data
//      .filter(p => !result.map(_._1).contains(p))
      .filter(p => isInRegion(point, p, toCalculatePoints))
      .map(p => Tuple2(p, countDominatedPoints(p, data))))
      .sortBy(_._2, ascending = false)

    val pointToAdd = toCalculatePoints.first()
    result :+ pointToAdd
    helper(topK - 1, pointToAdd._1, data.filter(p => !p.equals(pointToAdd)), result, newCalculatePoints)

  }

  //Finds if pointB is in the region of pointA
  def isInRegion(pointA: List[Double], pointB: List[Double], skylines: RDD[(List[Double], Long)]): Boolean = {
    skylines
      .map(_._1)
      .filter(p => !p.equals(pointA))
      .map(point => !Task1.dominates(point, pointB))
      .reduce(_&&_)
  }




}
