package org.example

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object Task1 {

  // --- Brute Force Solution---

  /**
   * Finds the skyline points using a brute force solution. First, we create a cartesian product for the input rdd with
   * itself, then we remove the pairs that have the same elements as first and second. After that, we group the pairs
   * based on the key and then using the function isSkyline, we find if the point is a skyline point or not.
   * @param data input data
   * @return rdd with skyline points
   */
  def skylinesBruteForce(data: RDD[List[Double]]): RDD[List[Double]] = {
    val answer = data.cartesian(data)
      .filter(pair => pair._1 != pair._2)
      .groupByKey()
      .filter(pair => isSkyline(pair._1, pair._2))
      .map(pair => pair._1)

    answer
  }

  // ------- ALS -------
  /**
   * Finds the manhattan distance of the point from the start (0,0,0,0...).
   * @param point the point we want to find its distance
   * @return the manhattan distance distance
   */
  def distanceFromStart(point: List[Double]): Double = {
    var sum = 0.0
    point.foreach(value => {
      sum += (value - 0).abs
    })

    sum
  }

  /**
   * Method to find the skyline points using the All Local Skylines method. First it finds the skylines points for each
   * partition (local skylines), using the SFS algorithm and then using the local skylines finds the global skylines points.
   * @param data input data
   * @return an iterator with the skyline points
   */
  def ALS(data: RDD[List[Double]]): Iterator[List[Double]] = {
    val localSkylines = data.mapPartitions(sfsForALS) //calculate the local skyline points in each partition
    val globalSkyline = sfsForALS(localSkylines.collect().toIterator) // calculate the global skyline points
    globalSkyline
  }

  /**
   * Implements the Sort-First Skyline (SFS) algorithm to find the skyline points.
   * @param data input data
   * @return skyline points
   */
  def sfsForALS(data: Iterator[List[Double]]): Iterator[List[Double]] = {
    //First sort points based on distance from 0,0,0,0
    val dataList = data
      .toList
      .map(point => Tuple2(point, distanceFromStart(point)))
      .sortBy(_._2)

    val skylineResult = ArrayBuffer[List[Double]]()

    skylineResult += dataList.head._1 //add the first element to the skyline result

    var i = 1
    while (i < dataList.length) {
      val p1 = dataList.apply(i)._1
      var flag = true //determines if a point should be added to the skyline result
      var j = 0
      breakable {
        while (j < skylineResult.length) { //check p1 with every element on the skyline result
          val p2 = skylineResult.apply(j)
          //if p1 dominates p2 then add the p1 to the skyline result and remove p2 from the result
          if (dominates(p1, p2)) {
            skylineResult.remove(j)
            j -= 1
          }
          //if p2 (which is already in the skyline result) dominates p1 then stop the loop,
          //since p1 is not a skyline point
          if (dominates(p2, p1)) {
            flag = false
            break()
          }
          j += 1
        }
        if (flag) skylineResult += p1 //add point to skylineResult
      }
      i += 1
    }

    skylineResult.toIterator
  }


  // ---- SFS -----
  /**
   * Same function as above, but with different inputs.
   * Implements the Sort-First Skyline (SFS) algorithm to find the skyline points.
   * @param data input data
   * @return skyline points
   */
  def SFS(data: RDD[List[Double]]): ArrayBuffer[List[Double]] = {
    //First sort points based on distance from 0,0,0,0
    val dataList = data
      .map(point => Tuple2(point, distanceFromStart(point)))
      .sortBy(_._2)
      .collect()

    val skyline = ArrayBuffer[List[Double]]()

    skyline +=  dataList.apply(0)._1 //add the first element to the skyline result

    var i = 1
    while (i < dataList.length) { //check p1 with every element on the skyline result
      val p1 = dataList.apply(i)._1
      var toBeAdded = true  //determines if a point should be added to the skyline result
      var j = 0
      breakable {
        while(j < skyline.length) {
          val p2 = skyline.apply(j)
          //if p1 dominates p2 then add the p1 to the skyline result and remove p2 from the result
          if(dominates(p1, p2)) {
            skyline.remove(j)
            j -= 1
          }
          //if p2 (which is already in the skyline result) dominates p1 then stop the loop,
          //since p1 is not a skyline point
          if (dominates(p2, p1)) {
            toBeAdded = false
            break()
          }
          j += 1
        }
        if(toBeAdded) skyline += p1 //add point to skylineResult
      }
      i += 1
    }
    skyline
  }

  //--- SALSA ----

  def SALSA(data: RDD[List[Double]]): ArrayBuffer[List[Double]] = {
    //First sort points based on the minimum dimension value
    val dataList = data
      .map(p => (p, p.min))
      .sortBy(pair => (pair._2, pair._1.sum), ascending = false)
      .collect()

    val skylineResult = ArrayBuffer[List[Double]]()

    //    skylineResult +=  dataList.apply(0)._1

    var pStop = Double.PositiveInfinity

    var i = 0
    breakable {
      while (i < dataList.length) {
        val p1 = dataList.apply(i)._1
        val pi = dataList.apply(i)._1.max
        val score = dataList.apply(i)._2
        var flag = true
        var j = 0

        while(j < skylineResult.length) {
          val p2 = skylineResult.apply(j)
          if(dominates(p1, p2)) {
            skylineResult.remove(j)
            j -= 1
          } else if (dominates(p2, p1)) {
            flag = false
            break()
          }
          j += 1
        }
        if(pStop <= score) {
          break()
        }
        if(flag) {
          skylineResult += p1
          pStop = Math.min(pi, pStop)
        }

        i += 1
      }
    }
    skylineResult
  }

  //----- Helper Functions -----
  /**
   * Checks if pointA dominates pointB
   * @param pointA the first point point
   * @param pointB the second point
   * @return true if pointA dominated pointB, otherwise it returns false
   */
  def dominates(pointA: List[Double], pointB: List[Double]): Boolean = {
    var dominates = false
    breakable {
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

  /**
   * Finds if point is a skyline point based on the values given
   * @param point the point we want to find if its skyline
   * @param values the dataset of the points we have to check
   * @return true if its skyline, otherwise false
   */
  def isSkyline(point: List[Double], values: Iterable[List[Double]]): Boolean = {
    values.foreach(nums => {
      if(isDominated(point, nums)) return false
    })
    true
  }

  /**
   * Checks if pointA is dominated by pointB
   * @param pointA the first point
   * @param pointB the second point
   * @return true if its dominated (therefore not skyline), otherwise false
   */
  def isDominated(pointA: List[Double], pointB: List[Double]): Boolean = {
    var isDominated = 0
    for( i <- pointA.indices) {
      if(pointB.apply(i) <= pointA.apply(i)) {
        isDominated += 1
      }
    }
    if(isDominated == pointA.size) {
      return true
    }
    false
  }
}