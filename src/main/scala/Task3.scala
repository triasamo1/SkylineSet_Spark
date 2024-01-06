package org.example

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object Task3 {

  // --- Brute Force Solution---

  /**
   * This is the brute force solution to find the top k skyline points based on their dominance score.
   * First,find the skyline points (using the function skylinesBruteForce) and then we map the points to a Tuple with
   * itself as first element and its dominance score as second. We calculate its dominance score using the function
   * countDominatedPoints where we give the input data as input alongside with the skyline point.
   * Lastly, we sort them in an descending order and we return the top k
   * @param data input data with all the points
   * @param top number of points we want to find
   * @return an array with the top dominating points and their dominance score
   */
  def topKSkylineBruteForce(data: RDD[List[Double]], top: Int): Array[(List[Double], Long)] = {
    Task1.skylinesBruteForce(data) //get skyline points
      .collect()
      .map(point => (point, countDominatedPoints(point, data))) //map them to a Tuple with point as first element and its dominance score as second
      .sortBy(-_._2) //sort elements in a descending order based on their dominance score
      .take(top) //take first 'top' elements
  }

  //----- Solution 2 -----

  /**
   * First, we broadcast the input data rdd, and then using the method ALS from Task1 we find the skyline points.
   * Then we map the skyline points to partitions and for each partition we calculate for its points their dominance
   * score. We calculate their dominance score using the function countDominatedPoints that takes as argument
   * the point and the broadcasted value. Lastly, we sort them in an descending order based on their dominance
   * score and we return the top k.
   * @param data input data with all the points
   * @param top number of points we want to find
   * @param sc spark context
   * @return an array with the top dominating points and their dominance score
   */
  def topKSkylinePoints(data: RDD[List[Double]], top: Int, sc: SparkContext): Array[(List[Double], Long)] = {
    val broadcastData = sc.broadcast(data.collect()) //broadcast data rdd
    val skylines = Task1.ALS(data).toList //find skyline points

    sc.parallelize(skylines)
      .mapPartitions(par => par //map to partitions
        .map(point => (point, countDominatedPoints(point, broadcastData)))) //for each point in the partition find its dominance score
      .sortBy(-_._2) //sort elements in a descending order based on their dominance score
      .take(top) //take first 'top' elements
  }

  //----- Solution 3 -----

  /**
   * First, we create a cartesian product of the skyline rdd (was found using function ALS from Task1) with
   * the input rdd, then we remove pairs that have the same element as second and first. After that, we find for every
   * pair if the first element dominates the second.If does we map the element to a tuple with itself and 1 if
   * it doesn't, we map the element to a tuple with itself and 0. Then we reduce the rdd by merging the values
   * of each key by adding every value for that key. We do that to find for its point its dominance score.
   * Lastly, we sort them in an descending order based on their dominance score and we return the top k
   * @param data input data with all the points
   * @param top number of points we want to find
   * @param sc spark context
   * @return an array with the top dominating points and their dominance score
   */
  def topKSkylinePoints2(data: RDD[List[Double]], top: Int, sc: SparkContext): Array[(List[Double], Long)] = {
    sc.parallelize(Task1.ALS(data).toList).cartesian(data)
      .filter(pair => pair._1 != pair._2)
      .map(pair => (pair._1, if (Task1.dominates(pair._1, pair._2)) 1L else 0L))
      .reduceByKey(_+_)
      .sortBy(-_._2)
      .take(top)
  }

  // --- Helper Functions ---

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

  def countDominatedPoints(point: List[Double], points: Iterable[List[Double]]): Long = {
    points
      .filter(p => !p.equals(point))
      .count(p => Task1.dominates(point, p))
  }

  def countDominatedPoints(point: List[Double], points: RDD[List[Double]]): Long = {
    points
      .filter(p => !p.equals(point))
      .filter(p => Task1.dominates(point, p))
      .count()
  }
}
