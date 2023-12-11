package org.example

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer


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
    values
      .filter(p => !p.equals(key))
      .foreach(nums => {
        if(Task1.dominates(key, nums)) totalPoints += 1
      })
    totalPoints
  }

  def STD(data: RDD[List[Double]], top: Int,  sc: SparkContext) = {
    if(top == 0)  RDD

    val skylines = Task1.sfs(data)
    val skylinePoints = skylines
      .map(point => Tuple2(point, countDominatedPoints2(point, data.collect())))
      .sortWith(_._2 > _._2)
//      .sortBy(_._2, ascending = false)


    val point = skylinePoints.apply(0)
    var result = Array(point)
    skylinePoints.remove(0)

    val answer = helper2(top - 1, point._1, data.filter(p => !p.equals(point)).collect(), result, skylinePoints)



    answer
//    data
//      .filter(p => !result.map(_._1).contains(p))
//      .filter(p => isInRegion(point._1, p, skylinePoints))
//      .map(p => Tuple2(p, countDominatedPoints(p, data)))


//    val point = skylinePoints.first()
//    result :+ point

//    helper(top, point._1, data.filter(p => !p.equals(point)), result, skylinePoints)

//    print(result)
//
//    result
//    data
//      .filter(p => !result.map(_._1).contains(p))
//      .filter(p => isInRegion(point._1, p, skylinePoints))
//      .map(p => Tuple2(p, countDominatedPoints(p, data)))
  }

  //TODO rename toCalculatePoints
  def helper2(topK: Int, point: List[Double], data: Array[List[Double]], result: Array[(List[Double], Long)],
             toCalculatePoints: ArrayBuffer[(List[Double], Long)]): Array[(List[Double], Long)] = {
    if(topK == 0)  return result

    var res = result
    val data2 = data
      .filter(p => !p.equals(point))
      .filter(p => !toCalculatePoints.map(_._1).contains(p))

    var temp = data2
      .filter(p => isInRegion2(point, p, toCalculatePoints))
      .map(p => Tuple2(p, countDominatedPoints2(p, data)))
      .foreach(p => toCalculatePoints.append(p))

    //Add points belonging only to region of point to the toCalculatePoints RDD
    val newCalculatePoints = toCalculatePoints
      .sortWith(_._2 > _._2)

    val pointToAdd = toCalculatePoints.apply(0)
    res :+= pointToAdd

    newCalculatePoints.remove(0)
    helper2(topK - 1, pointToAdd._1, data.filter(p => !p.equals(pointToAdd)), res, newCalculatePoints)

  }

  //Finds if pointB is in the region of pointA
  def isInRegion2(pointA: List[Double], pointB: List[Double], skylines: ArrayBuffer[(List[Double], Long)]): Boolean = {
    skylines
      .map(_._1)
      .filter(p => !p.equals(pointB))
      .map(point => !Task1.dominates(point, pointB))
      .reduce(_&&_)
  }


  def countDominatedPoints2(point: List[Double], points: Array[List[Double]]) : Long = {
    var totalPoints = 0
    points
      .filter(p => !p.equals(point))
      .foreach(nums => {
        if(Task1.dominates(point, nums)) totalPoints += 1
      })
    totalPoints
  }

  def countDominatedPoints(point: List[Double], points: RDD[List[Double]]) : Long = {
//    points
//      .map(p => Task1.dominates(point, p))
//      .count()
    var totalPoints = 0
    points
      .filter(p => !p.equals(point))
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
